// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Protocol.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// Tests for the Schema-Registry query-string extension to KafkaConnection.
/// The base parsing tests live in KafkaConnectionTests; this file pins
/// only the new behaviour around <c>?schema-registry=…</c>.
/// </summary>
public class KafkaConnectionSchemaRegistryTests
{
    [Fact]
    public void TryParse_PullsSchemaRegistryFromQueryString()
    {
        var ep = KafkaConnection.TryParse("kafka://broker:9092?schema-registry=http://sr:8081");
        Assert.NotNull(ep);
        Assert.Equal("broker:9092", ep!.Value.BootstrapServers);
        Assert.Equal("http://sr:8081", ep.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_AcceptsShortAlias_sr()
    {
        var ep = KafkaConnection.TryParse("kafka://broker:9092?sr=http://sr:8081");
        Assert.Equal("http://sr:8081", ep!.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_DoesNotConfuseCommasInBootstrapWithQueryString()
    {
        var ep = KafkaConnection.TryParse("kafka://b1:9092,b2:9092?schema-registry=http://sr:8081");
        Assert.Equal("b1:9092,b2:9092", ep!.Value.BootstrapServers);
        Assert.Equal("http://sr:8081", ep.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_NoQueryString_LeavesSchemaRegistryNull()
    {
        var ep = KafkaConnection.TryParse("kafka://broker:9092");
        Assert.Null(ep!.Value.SchemaRegistryUrl);
    }

    [Fact]
    public void TryParse_UrlEncodedSchemaRegistry_Decodes()
    {
        // Some users URL-encode the colon; the parser unescapes so the
        // resolved value is still a usable URL.
        var ep = KafkaConnection.TryParse("kafka://broker:9092?schema-registry=http%3A%2F%2Fsr%3A8081");
        Assert.Equal("http://sr:8081", ep!.Value.SchemaRegistryUrl);
    }
}
