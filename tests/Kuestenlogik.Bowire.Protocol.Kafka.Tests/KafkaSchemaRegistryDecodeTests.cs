// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Avro;
using Avro.Generic;
using Avro.IO;
using Kuestenlogik.Bowire.Protocol.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// End-to-end test for the Avro decode path: a fake Schema Registry HTTP
/// server (in-memory <see cref="HttpListener"/>) serves a minimal Avro
/// schema, the test encodes a payload with the Confluent wire format
/// (<c>0x00</c> magic + 4-byte schema id + Avro body), and
/// <see cref="KafkaSchemaRegistry.TryDecodeAsync"/> hands back the JSON
/// projection.
/// </summary>
public class KafkaSchemaRegistryDecodeTests
{
    private const string OrderSchemaJson = """
        {
          "type": "record",
          "name": "Order",
          "namespace": "test",
          "fields": [
            { "name": "id",    "type": "long" },
            { "name": "name",  "type": "string" },
            { "name": "total", "type": "double" }
          ]
        }
        """;

    [Fact]
    public async Task TryDecode_ConfluentWireFormatAvro_ReturnsJsonProjection()
    {
        const int schemaId = 42;

        await using var sr = await FakeSchemaRegistry.StartAsync(new Dictionary<int, (string Type, string Schema)>
        {
            [schemaId] = ("AVRO", OrderSchemaJson),
        }, new Dictionary<string, (int Id, int Version, string Type, string Schema)>());

        var payload = EncodeConfluentAvro(schemaId, OrderSchemaJson, new Dictionary<string, object?>
        {
            ["id"] = 42L,
            ["name"] = "Ada",
            ["total"] = 19.95,
        });

        using var registry = new KafkaSchemaRegistry(sr.Url);
        var json = await registry.TryDecodeAsync(payload);

        Assert.NotNull(json);
        using var doc = JsonDocument.Parse(json!);
        Assert.Equal(42L, doc.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("Ada", doc.RootElement.GetProperty("name").GetString());
        Assert.Equal(19.95, doc.RootElement.GetProperty("total").GetDouble());
    }

    [Fact]
    public async Task TryDecode_NoMagicByte_ReturnsNull()
    {
        // Plain UTF-8 JSON / opaque binary doesn't carry the framing
        // prefix; the registry shouldn't try to be clever.
        await using var sr = await FakeSchemaRegistry.StartAsync(
            new Dictionary<int, (string Type, string Schema)>(),
            new Dictionary<string, (int Id, int Version, string Type, string Schema)>());

        using var registry = new KafkaSchemaRegistry(sr.Url);
        Assert.Null(await registry.TryDecodeAsync(Encoding.UTF8.GetBytes("{\"raw\": true}")));
    }

    [Fact]
    public async Task TryDecode_ShortPayload_ReturnsNull()
    {
        await using var sr = await FakeSchemaRegistry.StartAsync(
            new Dictionary<int, (string Type, string Schema)>(),
            new Dictionary<string, (int Id, int Version, string Type, string Schema)>());

        using var registry = new KafkaSchemaRegistry(sr.Url);
        Assert.Null(await registry.TryDecodeAsync(new byte[] { 0x00, 0x01 }));
    }

    [Fact]
    public async Task TryGetLatest_KnownSubject_ReturnsRegisteredSchema()
    {
        await using var sr = await FakeSchemaRegistry.StartAsync(
            new Dictionary<int, (string Type, string Schema)>(),
            new Dictionary<string, (int Id, int Version, string Type, string Schema)>
            {
                ["orders-value"] = (1, 1, "AVRO", OrderSchemaJson),
            });

        using var registry = new KafkaSchemaRegistry(sr.Url);
        var schema = await registry.TryGetLatestAsync("orders-value");
        Assert.NotNull(schema);
        Assert.Equal(1, schema!.Id);
        Assert.Equal(1, schema.Version);
    }

    private static byte[] EncodeConfluentAvro(int schemaId, string schemaJson, Dictionary<string, object?> values)
    {
        var schema = (RecordSchema)Schema.Parse(schemaJson);
        var record = new GenericRecord(schema);
        foreach (var (k, v) in values) record.Add(k, v);

        using var bodyMs = new MemoryStream();
        var encoder = new BinaryEncoder(bodyMs);
        var writer = new GenericDatumWriter<object>(schema);
        writer.Write(record, encoder);
        encoder.Flush();
        var body = bodyMs.ToArray();

        // 5-byte framing prefix: 0x00 magic + 4-byte big-endian schema id.
        var payload = new byte[5 + body.Length];
        payload[0] = 0x00;
        payload[1] = (byte)((schemaId >> 24) & 0xFF);
        payload[2] = (byte)((schemaId >> 16) & 0xFF);
        payload[3] = (byte)((schemaId >> 8) & 0xFF);
        payload[4] = (byte)(schemaId & 0xFF);
        Buffer.BlockCopy(body, 0, payload, 5, body.Length);
        return payload;
    }

    /// <summary>
    /// Minimal Schema Registry stand-in for the decode tests. Implements
    /// the two endpoints CachedSchemaRegistryClient hits: GET schemas/ids/{id}
    /// and GET subjects/{subject}/versions/latest. Skips auth, security,
    /// and the dozen other endpoints — Bowire only reads.
    /// </summary>
    private sealed class FakeSchemaRegistry : IAsyncDisposable
    {
        private readonly HttpListener _listener;
        private readonly Task _loop;
        public string Url { get; }

        private FakeSchemaRegistry(HttpListener listener, string url, Task loop)
        {
            _listener = listener;
            Url = url;
            _loop = loop;
        }

        public static Task<FakeSchemaRegistry> StartAsync(
            Dictionary<int, (string Type, string Schema)> byId,
            Dictionary<string, (int Id, int Version, string Type, string Schema)> bySubject)
        {
            var port = GetFreePort();
            var url = $"http://127.0.0.1:{port}/";
            var listener = new HttpListener();
            listener.Prefixes.Add(url);
            listener.Start();

            var loop = Task.Run(async () =>
            {
                while (listener.IsListening)
                {
                    HttpListenerContext ctx;
                    try { ctx = await listener.GetContextAsync(); }
                    catch { break; }

                    var path = ctx.Request.Url!.AbsolutePath;
                    try
                    {
                        if (path.StartsWith("/schemas/ids/", StringComparison.Ordinal))
                        {
                            var idStr = path["/schemas/ids/".Length..];
                            if (int.TryParse(idStr, out var id) && byId.TryGetValue(id, out var entry))
                            {
                                await WriteJsonAsync(ctx, new { schema = entry.Schema, schemaType = entry.Type });
                            }
                            else
                            {
                                ctx.Response.StatusCode = 404;
                                await WriteJsonAsync(ctx, new { error_code = 40403, message = "Schema not found" });
                            }
                        }
                        else if (path.StartsWith("/subjects/", StringComparison.Ordinal) &&
                                 path.EndsWith("/versions/latest", StringComparison.Ordinal))
                        {
                            var subject = path["/subjects/".Length..^"/versions/latest".Length];
                            if (bySubject.TryGetValue(subject, out var entry))
                            {
                                await WriteJsonAsync(ctx, new
                                {
                                    subject,
                                    version = entry.Version,
                                    id = entry.Id,
                                    schema = entry.Schema,
                                    schemaType = entry.Type
                                });
                            }
                            else
                            {
                                ctx.Response.StatusCode = 404;
                                await WriteJsonAsync(ctx, new { error_code = 40401, message = "Subject not found" });
                            }
                        }
                        else
                        {
                            ctx.Response.StatusCode = 404;
                            ctx.Response.Close();
                        }
                    }
                    catch
                    {
                        try { ctx.Response.StatusCode = 500; ctx.Response.Close(); } catch { }
                    }
                }
            });

            return Task.FromResult(new FakeSchemaRegistry(listener, url, loop));
        }

        private static async Task WriteJsonAsync(HttpListenerContext ctx, object payload)
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(payload);
            ctx.Response.ContentType = "application/vnd.schemaregistry.v1+json";
            ctx.Response.ContentLength64 = bytes.Length;
            await ctx.Response.OutputStream.WriteAsync(bytes);
            ctx.Response.Close();
        }

        private static int GetFreePort()
        {
            using var l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            var p = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return p;
        }

        public async ValueTask DisposeAsync()
        {
            try { _listener.Stop(); } catch { }
            try { _listener.Close(); } catch { }
            try { await _loop; } catch { }
        }
    }
}
