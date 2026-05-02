// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// Unit-level checks that don't require a live broker. Broker-backed
/// integration tests need Testcontainers-Kafka or an external broker;
/// those can follow later when the plugin gets a CI lane with Docker.
/// </summary>
public sealed class BowireKafkaProtocolTests
{
    [Fact]
    public async Task Discover_WithMalformedUrl_ReturnsEmpty()
    {
        var plugin = new BowireKafkaProtocol();
        var services = await plugin.DiscoverAsync("http://example.com", false, TestContext.Current.CancellationToken);
        Assert.Empty(services);
    }

    [Fact]
    public async Task Discover_WithUnreachableBroker_ReturnsEmpty()
    {
        // 1.2.3.4:19999 on a random high port — not a real broker,
        // and far enough from the common ports that nothing on the
        // test host will answer. The admin client's GetMetadata
        // throws KafkaException, which we swallow so the sidebar
        // stays clean instead of crashing the discovery pass.
        var plugin = new BowireKafkaProtocol();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var services = await plugin.DiscoverAsync("kafka://127.0.0.1:1", false, cts.Token);
        Assert.Empty(services);
    }

    [Fact]
    public async Task InvokeAsync_ConsumeMethod_ReturnsHelpfulError()
    {
        var plugin = new BowireKafkaProtocol();
        var result = await plugin.InvokeAsync(
            "kafka://127.0.0.1:9092", "my-topic", BowireKafkaProtocol.ConsumeMethodName,
            ["{}"], false, null, TestContext.Current.CancellationToken);
        Assert.Contains("consume", result.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task InvokeAsync_InvalidUrl_ReturnsError()
    {
        var plugin = new BowireKafkaProtocol();
        var result = await plugin.InvokeAsync(
            "http://not-kafka", "my-topic", BowireKafkaProtocol.ProduceMethodName,
            ["{}"], false, null, TestContext.Current.CancellationToken);
        Assert.Contains("Invalid", result.Status, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("__consumer_offsets", true)]
    [InlineData("__transaction_state", true)]
    [InlineData("_schemas", true)]
    [InlineData("_confluent-ksql", true)]
    [InlineData("orders", false)]
    [InlineData("payments.events", false)]
    public void IsInternalTopic_ClassifiesByPrefix(string topic, bool expected)
    {
        Assert.Equal(expected, BowireKafkaProtocol.IsInternalTopic(topic));
    }

    [Fact]
    public void TryDecodeUtf8_NullInput_ReturnsNull()
    {
        Assert.Null(BowireKafkaProtocol.TryDecodeUtf8(null));
    }

    [Fact]
    public void TryDecodeUtf8_EmptyInput_ReturnsEmptyString()
    {
        Assert.Equal(string.Empty, BowireKafkaProtocol.TryDecodeUtf8([]));
    }

    [Fact]
    public void TryDecodeUtf8_ValidUtf8_ReturnsDecoded()
    {
        var bytes = Encoding.UTF8.GetBytes("Grüße aus der Küche");
        Assert.Equal("Grüße aus der Küche", BowireKafkaProtocol.TryDecodeUtf8(bytes));
    }

    [Fact]
    public void TryDecodeUtf8_BinaryBytes_ReturnsNull()
    {
        // Lone 0xFF is not a valid UTF-8 lead byte on its own.
        Assert.Null(BowireKafkaProtocol.TryDecodeUtf8([0xFF, 0xFE]));
    }

    [Fact]
    public void Settings_ExposesExpectedKnobs()
    {
        var plugin = new BowireKafkaProtocol();
        var keys = plugin.Settings.Select(s => s.Key).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("discoveryTimeoutSeconds", keys);
        Assert.Contains("consumerGroupPrefix", keys);
    }

    [Fact]
    public void IdentityProperties_MatchBowireConventions()
    {
        var plugin = new BowireKafkaProtocol();
        Assert.Equal("kafka", plugin.Id);
        Assert.Equal("Kafka", plugin.Name);
        Assert.Contains("svg", plugin.IconSvg, StringComparison.Ordinal);
    }
}
