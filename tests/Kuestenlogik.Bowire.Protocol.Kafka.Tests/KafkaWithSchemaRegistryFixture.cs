// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// Brings up a Kafka container <em>and</em> a Confluent Schema Registry
/// container on a shared Docker network so they can talk to each other
/// over the network alias <c>kafka</c>. The Bowire tests reach Kafka
/// via the host-mapped bootstrap address (Testcontainers handles the
/// listener split internally) and Schema Registry via the host-mapped
/// HTTP port.
/// </summary>
public sealed class KafkaWithSchemaRegistryFixture : IAsyncLifetime
{
    private readonly INetwork _network = new NetworkBuilder().Build();

    private readonly KafkaContainer _kafka;
    private readonly IContainer _schemaRegistry;

    public KafkaWithSchemaRegistryFixture()
    {
        _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.7.1")
            .WithNetwork(_network)
            .WithNetworkAliases("kafka")
            .Build();

        _schemaRegistry = new ContainerBuilder("confluentinc/cp-schema-registry:7.7.1")
            .WithNetwork(_network)
            .WithNetworkAliases("schema-registry")
            .WithPortBinding(8081, true) // 0 → random host port
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            // Internal Kafka listener — KafkaContainer publishes both an
            // external (host) listener and an internal one named BROKER
            // on port 9093. Schema Registry has to use the internal one
            // because they share the Docker network.
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9093")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPath("/subjects").ForPort(8081)))
            .Build();
    }

    public string BootstrapServers => _kafka.GetBootstrapAddress();

    public string SchemaRegistryUrl =>
        $"http://{_schemaRegistry.Hostname}:{_schemaRegistry.GetMappedPublicPort(8081)}";

    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync();
        await _kafka.StartAsync();
        await _schemaRegistry.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _schemaRegistry.DisposeAsync();
        await _kafka.DisposeAsync();
        await _network.DisposeAsync();
    }
}
