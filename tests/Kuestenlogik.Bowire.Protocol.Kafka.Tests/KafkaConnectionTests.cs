// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

public sealed class KafkaConnectionTests
{
    [Fact]
    public void TryParse_SingleBrokerUrl_NormalisesToHostPort()
    {
        var endpoint = KafkaConnection.TryParse("kafka://broker.example:9092");
        Assert.NotNull(endpoint);
        Assert.Equal("broker.example:9092", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_BareHostPort_AcceptedWithoutScheme()
    {
        var endpoint = KafkaConnection.TryParse("broker.example:9094");
        Assert.NotNull(endpoint);
        Assert.Equal("broker.example:9094", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_MultipleBrokers_PreservesCsv()
    {
        var endpoint = KafkaConnection.TryParse("kafka://b1:9092,b2:9092,b3:9092");
        Assert.NotNull(endpoint);
        Assert.Equal("b1:9092,b2:9092,b3:9092", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_HostWithoutPort_AppliesDefault()
    {
        var endpoint = KafkaConnection.TryParse("kafka://broker.example");
        Assert.NotNull(endpoint);
        Assert.Equal("broker.example:9092", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_WithWhitespaceAndTrailingComma_NormalisesCleanly()
    {
        var endpoint = KafkaConnection.TryParse("kafka:// b1:9092 , b2:9093 ,");
        Assert.NotNull(endpoint);
        Assert.Equal("b1:9092,b2:9093", endpoint!.Value.BootstrapServers);
    }

    [Fact]
    public void TryParse_NonKafkaScheme_ReturnsNull()
    {
        Assert.Null(KafkaConnection.TryParse("https://example.com"));
    }

    [Fact]
    public void TryParse_Empty_ReturnsNull()
    {
        Assert.Null(KafkaConnection.TryParse(""));
        Assert.Null(KafkaConnection.TryParse(null));
    }
}
