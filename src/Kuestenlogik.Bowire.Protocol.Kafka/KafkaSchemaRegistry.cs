// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text;
using Confluent.SchemaRegistry;

namespace Kuestenlogik.Bowire.Protocol.Kafka;

/// <summary>
/// Thin wrapper around <see cref="CachedSchemaRegistryClient"/> + the
/// Confluent wire-format decoder. Loads schemas for <c>&lt;topic&gt;-key</c>
/// and <c>&lt;topic&gt;-value</c> subjects on demand and caches them so
/// every consumed message doesn't pay another HTTP round-trip.
/// <para>
/// Bowire only deals with the discovery + display side of Schema Registry
/// — the mock server stays schema-agnostic. Replay re-emits whatever bytes
/// got captured during the original consume; that keeps the mock-replay
/// path simple and lossless even for binary Avro / Protobuf payloads.
/// </para>
/// </summary>
internal sealed class KafkaSchemaRegistry : IDisposable
{
    private readonly CachedSchemaRegistryClient _client;
    private readonly ConcurrentDictionary<int, RegisteredSchema> _byId = new();
    private readonly ConcurrentDictionary<string, RegisteredSchema?> _bySubject = new(StringComparer.Ordinal);

    public string Url { get; }

    public KafkaSchemaRegistry(string url)
    {
        Url = url;
        _client = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = url,
            // Aggressive client-side cache caps in the dev-tool context.
            MaxCachedSchemas = 1000,
            RequestTimeoutMs = 5_000,
        });
    }

    /// <summary>
    /// Look up the latest schema registered against <paramref name="subject"/>
    /// (typically <c>&lt;topic&gt;-value</c> or <c>&lt;topic&gt;-key</c>).
    /// Returns <c>null</c> when the subject is missing — schemaless topics
    /// are common in dev clusters and the discovery layer treats absence
    /// as "no schema, fall back to raw bytes".
    /// </summary>
    public async Task<RegisteredSchema?> TryGetLatestAsync(string subject)
    {
        if (_bySubject.TryGetValue(subject, out var cached)) return cached;
        try
        {
            var registered = await _client.GetLatestSchemaAsync(subject).ConfigureAwait(false);
            _bySubject[subject] = registered;
            if (registered is not null) _byId[registered.Id] = registered;
            return registered;
        }
        catch (SchemaRegistryException)
        {
            _bySubject[subject] = null;
            return null;
        }
    }

    /// <summary>
    /// Decode a Confluent wire-format payload (<c>0x00</c> magic byte +
    /// 4-byte big-endian schema id + Avro/JSON/Protobuf body) into a
    /// human-readable string. Returns <c>null</c> when the bytes don't
    /// carry the framing prefix at all (plain UTF-8 / opaque binary), so
    /// the caller can fall back to UTF-8 + base64 rendering.
    /// </summary>
    public async Task<string?> TryDecodeAsync(byte[]? payload)
    {
        if (payload is null || payload.Length < 5) return null;
        if (payload[0] != 0x00) return null;
        var schemaId = (payload[1] << 24) | (payload[2] << 16) | (payload[3] << 8) | payload[4];
        var body = new byte[payload.Length - 5];
        Buffer.BlockCopy(payload, 5, body, 0, body.Length);

        var schema = await TryGetByIdAsync(schemaId).ConfigureAwait(false);
        if (schema is null) return null;

        return schema.SchemaType switch
        {
            // JSON Schema and Protobuf payloads aren't decoded here yet —
            // returning the raw body as UTF-8 lets the workbench at least
            // surface human-readable JSON Schema payloads cleanly. Avro
            // is the priority for the first slice; the other two are
            // straightforward extensions.
            SchemaType.Json => DecodeJsonBody(body),
            SchemaType.Avro => DecodeAvroBody(body, schema.SchemaString),
            _ => null,
        };
    }

    private async Task<RegisteredSchema?> TryGetByIdAsync(int schemaId)
    {
        if (_byId.TryGetValue(schemaId, out var cached)) return cached;
        try
        {
            var schemaString = await _client.GetSchemaAsync(schemaId).ConfigureAwait(false);
            // GetSchemaAsync returns just the schema string + type; wrap
            // it in a synthetic RegisteredSchema so the by-id cache
            // keeps the same shape as by-subject lookups.
            var registered = new RegisteredSchema(
                subject: string.Empty,
                version: 0,
                id: schemaId,
                schemaString: schemaString.SchemaString,
                schemaType: schemaString.SchemaType,
                references: schemaString.References ?? []);
            _byId[schemaId] = registered;
            return registered;
        }
        catch (SchemaRegistryException)
        {
            return null;
        }
    }

    private static string DecodeJsonBody(byte[] body)
    {
        // Confluent's JSON Schema serializer puts the JSON document
        // straight into the body after the framing prefix.
        return Encoding.UTF8.GetString(body);
    }

    private static string? DecodeAvroBody(byte[] body, string schemaJson)
    {
        try
        {
            var schema = Avro.Schema.Parse(schemaJson);
            using var ms = new MemoryStream(body);
            var decoder = new Avro.IO.BinaryDecoder(ms);
            var reader = new Avro.Generic.GenericDatumReader<object>(schema, schema);
            var obj = reader.Read(reuse: null!, decoder);
            return AvroValueToJson.Serialize(obj);
        }
        catch
        {
            return null;
        }
    }

    public void Dispose() => _client.Dispose();
}
