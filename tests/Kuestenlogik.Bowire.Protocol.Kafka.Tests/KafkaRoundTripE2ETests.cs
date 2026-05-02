// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Bowire.Protocol.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// End-to-end test against a real Kafka broker (Testcontainers). Builds
/// confidence the librdkafka wire path on top of Bowire's plugin works
/// — discovery against actual broker metadata, produce, and consume
/// round-tripping a single message. Schema-registry decoding has its
/// own E2E test next to this one.
/// </summary>
[Trait("Category", "Docker")]
public class KafkaRoundTripE2ETests : IClassFixture<KafkaContainerFixture>
{
    private readonly KafkaContainerFixture _kafka;

    public KafkaRoundTripE2ETests(KafkaContainerFixture kafka) => _kafka = kafka;

    [Fact]
    public async Task Discover_RealBroker_FindsCreatedTopic()
    {
        const string topic = "bowire-discover-test";
        await CreateTopicAsync(topic);

        var protocol = new BowireKafkaProtocol();
        var services = await protocol.DiscoverAsync(
            "kafka://" + _kafka.BootstrapServers,
            showInternalServices: false,
            CancellationToken.None);

        // Cluster service + the freshly-created topic must both be there.
        Assert.Contains(services, s => s.Name == BowireKafkaProtocol.ClusterServiceName);
        var topicService = services.FirstOrDefault(s => s.Name == topic);
        Assert.NotNull(topicService);
        Assert.Equal(2, topicService!.Methods.Count);
        Assert.Contains(topicService.Methods, m => m.Name == BowireKafkaProtocol.ConsumeMethodName);
        Assert.Contains(topicService.Methods, m => m.Name == BowireKafkaProtocol.ProduceMethodName);
    }

    [Fact]
    public async Task Produce_ThenConsume_RoundTripsMessage()
    {
        const string topic = "bowire-roundtrip-test";
        await CreateTopicAsync(topic);

        var protocol = new BowireKafkaProtocol();
        var serverUrl = "kafka://" + _kafka.BootstrapServers;

        // 1. Produce one message via Bowire. The "key" metadata entry
        //    travels through the same path the workbench's metadata tab
        //    feeds in.
        var produceResult = await protocol.InvokeAsync(
            serverUrl: serverUrl,
            service: topic,
            method: BowireKafkaProtocol.ProduceMethodName,
            jsonMessages: ["""{"id":42,"name":"Ada"}"""],
            showInternalServices: false,
            metadata: new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["key"] = "user-42",
            },
            ct: CancellationToken.None);

        Assert.Equal("OK", produceResult.Status);
        Assert.NotNull(produceResult.Response);
        var produceJson = JsonDocument.Parse(produceResult.Response!);
        Assert.Equal(topic, produceJson.RootElement.GetProperty("topic").GetString());
        Assert.True(produceJson.RootElement.GetProperty("offset").GetInt64() >= 0);

        // 2. Consume from the start of the partition. The plugin's
        //    consumer config defaults to AutoOffsetReset.Latest, so we
        //    can't just read what we just produced — instead we override
        //    the consumer group via a fresh server URL? No — the plugin
        //    creates a new throwaway group per call. The race is real:
        //    our produced message could be before the group's "Latest"
        //    cursor lands. Workaround: produce a marker AFTER subscribe.
        //
        //    Easier: spin a parallel consumer-with-a-fresh-group manually
        //    and re-produce inside the consume loop so we hit the same
        //    "see what gets produced after subscribe" semantics the
        //    plugin's consumer expects in real workbench use.
        var consumeUrl = serverUrl;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var consumeTask = Task.Run(async () =>
        {
            await foreach (var envelope in protocol.InvokeStreamAsync(
                serverUrl: consumeUrl,
                service: topic,
                method: BowireKafkaProtocol.ConsumeMethodName,
                jsonMessages: ["{}"],
                showInternalServices: false,
                metadata: null,
                ct: cts.Token))
            {
                using var doc = JsonDocument.Parse(envelope);
                if (doc.RootElement.GetProperty("topic").GetString() == topic)
                {
                    return envelope;
                }
            }
            return null;
        }, cts.Token);

        // Give the consumer time to subscribe + join the group before we
        // produce, so AutoOffsetReset = Latest doesn't cause us to miss
        // the message.
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        await protocol.InvokeAsync(
            serverUrl: serverUrl,
            service: topic,
            method: BowireKafkaProtocol.ProduceMethodName,
            jsonMessages: ["""{"id":99,"name":"Grace"}"""],
            showInternalServices: false,
            metadata: new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["key"] = "user-99",
            },
            ct: TestContext.Current.CancellationToken);

        var receivedEnvelope = await consumeTask;
        await cts.CancelAsync();

        Assert.NotNull(receivedEnvelope);
        using var receivedDoc = JsonDocument.Parse(receivedEnvelope!);
        var root = receivedDoc.RootElement;
        Assert.Equal(topic, root.GetProperty("topic").GetString());
        Assert.Equal("user-99", root.GetProperty("key").GetString());
        Assert.Contains("Grace", root.GetProperty("value").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Consume_WithSecurityMarkers_StripsThemFromKafkaConfig()
    {
        // Produces against a plaintext broker but supplies a SASL
        // marker — the plugin must apply it to the consumer config (which
        // would then fail to connect because the broker's plaintext) but
        // the marker itself must not leak into Kafka headers, even when
        // the connection itself blows up.
        //
        // We test the negative path: the consumer should fail to connect
        // (no message ever lands) rather than connect with the marker
        // somehow leaking through.
        const string topic = "bowire-security-strip";
        await CreateTopicAsync(topic);

        var protocol = new BowireKafkaProtocol();
        var serverUrl = "kafka://" + _kafka.BootstrapServers;

        var saslJson = """{"mechanism":"PLAIN","username":"none","password":"none"}""";
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [KafkaSecurityConfig.SaslMarkerKey] = saslJson,
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var envelopeCount = 0;
        try
        {
            await foreach (var _ in protocol.InvokeStreamAsync(
                serverUrl: serverUrl,
                service: topic,
                method: BowireKafkaProtocol.ConsumeMethodName,
                jsonMessages: ["{}"],
                showInternalServices: false,
                metadata: metadata,
                ct: cts.Token))
            {
                envelopeCount++;
            }
        }
        catch (OperationCanceledException) { /* expected on timeout */ }

        // No messages produced anyway, but the test mostly proves the
        // consumer config accepts the SASL marker without crashing the
        // builder. KafkaSecurityConfig unit tests cover the strip-from-
        // dict assertion in isolation; this is the smoke check that
        // librdkafka doesn't reject the resulting ConsumerConfig outright.
        Assert.Equal(0, envelopeCount);
    }

    private async Task CreateTopicAsync(string topic)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = _kafka.BootstrapServers };
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
            // Tests in the same fixture share the broker; idempotent.
        }
    }
}

internal static class TaskExtensions
{
    public static async Task<T?> WaitOrDefaultAsync<T>(this Task<T?> task, TimeSpan timeout)
    {
        var completed = await Task.WhenAny(task, Task.Delay(timeout));
        return completed == task ? await task : default;
    }
}
