// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Testcontainers.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// xUnit class fixture that brings a single Confluent Kafka container
/// up once per test class and tears it down at the end. The image runs
/// in KRaft mode (no Zookeeper container needed) and binds to a
/// throwaway host port — the resulting <see cref="BootstrapServers"/>
/// is what the Bowire plugin and the test producers point at.
/// <para>
/// Tests that consume this fixture will fail-on-start if Docker isn't
/// running on the test host. That's by design: the suite is real-broker
/// E2E, locally you need Docker, on CI either provide Docker or filter
/// the tests out via the <c>Docker</c> trait.
/// </para>
/// </summary>
public sealed class KafkaContainerFixture : IAsyncLifetime
{
    // Pin a recent stable Confluent image. Bowire tests against the
    // GA wire format only; bumping this is cheap.
    private readonly KafkaContainer _container = new KafkaBuilder("confluentinc/cp-kafka:7.7.1")
        .Build();

    public string BootstrapServers => _container.GetBootstrapAddress();

    public ValueTask InitializeAsync() => new(_container.StartAsync());

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}
