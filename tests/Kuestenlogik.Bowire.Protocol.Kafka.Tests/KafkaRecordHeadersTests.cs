// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// #93 — the metadata of a produce goes out as record headers, but for the
/// Bowire markers, which can hold secrets, and the <c>key</c> and
/// <c>partition</c> the plugin addresses the record with. A consumed record
/// shows the headers it carries.
/// </summary>
public sealed class KafkaRecordHeadersTests
{
    [Fact]
    public void Metadata_Becomes_Headers_Without_Markers_Key_Or_Partition()
    {
        var headers = KafkaRecordHeaders.FromMetadata(new Dictionary<string, string>
        {
            ["trace-id"] = "abc",
            ["key"] = "user-1",
            ["partition"] = "0",
            ["__bowireKafkaSasl__"] = "{\"password\":\"secret\"}",
            ["tenant"] = "harbor",
        });

        Assert.NotNull(headers);
        Assert.Equal(
            [("trace-id", "abc"), ("tenant", "harbor")],
            headers!.Select(h => (h.Key, Encoding.UTF8.GetString(h.GetValueBytes()))));
    }

    [Fact]
    public void No_Header_Worth_Sending_Means_No_Headers()
    {
        Assert.Null(KafkaRecordHeaders.FromMetadata(null));
        Assert.Null(KafkaRecordHeaders.FromMetadata(new Dictionary<string, string> { ["key"] = "k" }));
    }

    [Fact]
    public void A_Consumed_Record_Lists_Its_Headers_In_Order_With_Repeats_And_Binary_As_Base64()
    {
        var headers = new Headers
        {
            { "trace-id", Encoding.UTF8.GetBytes("abc") },
            { "trace-id", Encoding.UTF8.GetBytes("def") },
            { "blob", [0xFF, 0xFE] },
        };

        var json = JsonSerializer.Serialize(KafkaRecordHeaders.ToEnvelope(headers));
        using var doc = JsonDocument.Parse(json);
        var items = doc.RootElement.EnumerateArray().ToList();

        Assert.Equal(["trace-id", "trace-id", "blob"], items.Select(i => i.GetProperty("key").GetString()));
        Assert.Equal("def", items[1].GetProperty("value").GetString());
        Assert.Equal(JsonValueKind.Null, items[2].GetProperty("value").ValueKind);
        Assert.Equal("//4=", items[2].GetProperty("valueBase64").GetString());
        Assert.Null(KafkaRecordHeaders.ToEnvelope(null));
    }
}

/// <summary>#93 against a real broker: produce with a header, read the record back.</summary>
[Trait("Category", "Docker")]
public sealed class KafkaRecordHeadersE2ETests(KafkaContainerFixture kafka) : IClassFixture<KafkaContainerFixture>
{
    [Fact]
    public async Task A_Produced_Header_Arrives_On_The_Record_And_In_The_Consumed_Envelope()
    {
        const string topic = "bowire-headers-test";
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.BootstrapServers }).Build())
        {
            try
            {
                await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
            }
            catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
            }
        }

        var protocol = new BowireKafkaProtocol();
        var produced = await protocol.InvokeAsync(
            "kafka://" + kafka.BootstrapServers, topic, BowireKafkaProtocol.ProduceMethodName,
            ["""{"id":1}"""], false,
            new Dictionary<string, string> { ["key"] = "k1", ["trace-id"] = "abc-123", ["__bowireMtls__"] = "secret" },
            TestContext.Current.CancellationToken);
        Assert.Equal("OK", produced.Status);
        var offset = long.Parse(produced.Metadata["offset"], System.Globalization.CultureInfo.InvariantCulture);
        var partition = int.Parse(produced.Metadata["partition"], System.Globalization.CultureInfo.InvariantCulture);

        // Read exactly that record back: assigned at its offset, so no
        // consumer group has to join before the produce.
        using var consumer = new ConsumerBuilder<byte[]?, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = "bowire-headers-test-" + Guid.NewGuid().ToString("N"),
            EnableAutoCommit = false,
        }).Build();
        consumer.Assign(new TopicPartitionOffset(topic, partition, offset));
        var record = consumer.Consume(TimeSpan.FromSeconds(20));

        Assert.NotNull(record);
        var header = Assert.Single(record!.Message.Headers);
        Assert.Equal("trace-id", header.Key);
        Assert.Equal("abc-123", Encoding.UTF8.GetString(header.GetValueBytes()));

        using var envelope = JsonDocument.Parse(await BowireKafkaProtocol.BuildEnvelopeAsync(record, registry: null));
        var shown = Assert.Single(envelope.RootElement.GetProperty("headers").EnumerateArray());
        Assert.Equal("abc-123", shown.GetProperty("value").GetString());
    }
}
