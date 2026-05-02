// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kuestenlogik.Bowire.Protocol.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// End-to-end test for the Schema Registry decode path against real
/// Confluent components (Kafka + cp-schema-registry containers). An
/// out-of-band producer publishes a Confluent-Avro-framed message; the
/// Bowire plugin's consume stream resolves the schema via the registry
/// and yields the decoded JSON projection through the Bowire envelope.
/// Complements the in-memory <see cref="KafkaSchemaRegistryDecodeTests"/>
/// which mocks the registry's HTTP API.
/// </summary>
[Trait("Category", "Docker")]
public class KafkaSchemaRegistryE2ETests : IClassFixture<KafkaWithSchemaRegistryFixture>
{
    private readonly KafkaWithSchemaRegistryFixture _fx;

    public KafkaSchemaRegistryE2ETests(KafkaWithSchemaRegistryFixture fx) => _fx = fx;

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
    public async Task Consume_AvroFramedMessage_DecodesViaSchemaRegistry()
    {
        const string topic = "bowire-sr-orders";
        await CreateTopicAsync(topic);

        // 1. External producer using Confluent's serdes — encodes one
        //    Avro message into the Confluent wire format and registers
        //    the schema under "<topic>-value".
        var srConfig = new SchemaRegistryConfig { Url = _fx.SchemaRegistryUrl };
        using var schemaRegistryClient = new CachedSchemaRegistryClient(srConfig);

        var producerConfig = new ProducerConfig { BootstrapServers = _fx.BootstrapServers };
        using var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistryClient))
            .Build();

        var avroSchema = (RecordSchema)Avro.Schema.Parse(OrderSchemaJson);
        var record = new GenericRecord(avroSchema);
        record.Add("id", 42L);
        record.Add("name", "Ada");
        record.Add("total", 19.95);

        // 2. Spin up the Bowire consumer with the schema-registry query
        //    parameter — this is the path the workbench takes when the
        //    user pastes "kafka://broker?schema-registry=…".
        var serverUrl = $"kafka://{_fx.BootstrapServers}?schema-registry={Uri.EscapeDataString(_fx.SchemaRegistryUrl)}";
        var protocol = new BowireKafkaProtocol();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var consumeTask = Task.Run(async () =>
        {
            await foreach (var envelope in protocol.InvokeStreamAsync(
                serverUrl: serverUrl,
                service: topic,
                method: BowireKafkaProtocol.ConsumeMethodName,
                jsonMessages: ["{}"],
                showInternalServices: false,
                metadata: null,
                ct: cts.Token))
            {
                using var doc = JsonDocument.Parse(envelope);
                if (doc.RootElement.GetProperty("topic").GetString() == topic)
                    return envelope;
            }
            return null;
        }, cts.Token);

        // Give the consumer time to subscribe + join its group before
        // we produce so AutoOffsetReset = Latest doesn't drop the
        // message under the cursor.
        await Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);

        await producer.ProduceAsync(topic, new Message<string, GenericRecord>
        {
            Key = "user-42",
            Value = record,
        }, TestContext.Current.CancellationToken);
        producer.Flush(TimeSpan.FromSeconds(5));

        var receivedEnvelope = await consumeTask;
        await cts.CancelAsync();

        Assert.NotNull(receivedEnvelope);
        using var receivedDoc = JsonDocument.Parse(receivedEnvelope!);
        var root = receivedDoc.RootElement;

        // The plugin tagged it as Avro decoded from the wire format.
        Assert.Equal("avro", root.GetProperty("encoding").GetString());

        // The "value" field carries the decoded JSON projection.
        var valueText = root.GetProperty("value").GetString();
        Assert.NotNull(valueText);
        using var valueDoc = JsonDocument.Parse(valueText!);
        Assert.Equal(42L, valueDoc.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("Ada", valueDoc.RootElement.GetProperty("name").GetString());
        Assert.Equal(19.95, valueDoc.RootElement.GetProperty("total").GetDouble());

        // The original wire bytes are still in valueBase64 — losslessly
        // preserved so mock-replay can re-emit them byte-for-byte.
        Assert.NotEmpty(root.GetProperty("valueBase64").GetString()!);
    }

    [Fact]
    public async Task Discover_TopicWithRegisteredSchema_AppendsSchemaSummary()
    {
        const string topic = "bowire-sr-discovery";
        await CreateTopicAsync(topic);

        // Register a schema for the topic via the SR client first so
        // discovery has something to find.
        using var schemaRegistryClient = new CachedSchemaRegistryClient(
            new SchemaRegistryConfig { Url = _fx.SchemaRegistryUrl });
        await schemaRegistryClient.RegisterSchemaAsync(
            $"{topic}-value",
            new Confluent.SchemaRegistry.Schema(OrderSchemaJson, SchemaType.Avro));

        var serverUrl = $"kafka://{_fx.BootstrapServers}?schema-registry={Uri.EscapeDataString(_fx.SchemaRegistryUrl)}";
        var protocol = new BowireKafkaProtocol();

        var services = await protocol.DiscoverAsync(
            serverUrl, showInternalServices: false, CancellationToken.None);

        var topicService = services.FirstOrDefault(s => s.Name == topic);
        Assert.NotNull(topicService);
        // Description should mention the registered Avro schema.
        Assert.Contains("Avro", topicService!.Description ?? string.Empty, StringComparison.Ordinal);
        Assert.Contains("value=", topicService.Description ?? string.Empty, StringComparison.Ordinal);
    }

    private async Task CreateTopicAsync(string topic)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = _fx.BootstrapServers };
        using var admin = new AdminClientBuilder(adminConfig).Build();
        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 },
            });
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Idempotent; tests in the same fixture share the broker.
        }
    }
}
