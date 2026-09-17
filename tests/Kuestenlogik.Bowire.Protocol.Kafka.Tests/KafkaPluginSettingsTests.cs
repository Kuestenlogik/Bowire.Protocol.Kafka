// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// The two settings this plugin declares reach the code that acts on them.
/// </summary>
/// <remarks>
/// They were declared from the first release and read by nothing: the
/// workbench wrote the value, the value survived a reload, and discovery
/// went on waiting five seconds. Nothing in the UI said otherwise, which
/// is what makes this worth a test rather than a comment — the same gap
/// DIS closed in Kuestenlogik/Bowire#640.
/// </remarks>
public sealed class KafkaPluginSettingsTests
{
    [Fact]
    public void Without_a_settings_store_the_defaults_stand()
    {
        var plugin = new BowireKafkaProtocol();

        // No Initialize at all — the CLI's case.
        Assert.Equal(TimeSpan.FromSeconds(5), plugin.DiscoveryTimeout());
        Assert.Equal("bowire", plugin.ConsumerGroupPrefix());
    }

    [Fact]
    public void A_host_without_the_service_is_the_same_as_no_host()
    {
        var plugin = new BowireKafkaProtocol();
        plugin.Initialize(new EmptyServiceProvider());

        Assert.Equal(TimeSpan.FromSeconds(5), plugin.DiscoveryTimeout());
        Assert.Equal("bowire", plugin.ConsumerGroupPrefix());
    }

    [Fact]
    public void The_workspaces_discovery_timeout_is_what_discovery_waits()
    {
        var plugin = new BowireKafkaProtocol();
        plugin.Initialize(new FakePluginSettings(("discoveryTimeoutSeconds", "30")));

        Assert.Equal(TimeSpan.FromSeconds(30), plugin.DiscoveryTimeout());
    }

    [Fact]
    public void The_workspaces_group_prefix_is_what_a_consume_group_is_named()
    {
        var plugin = new BowireKafkaProtocol();
        plugin.Initialize(new FakePluginSettings(("consumerGroupPrefix", "ops-team")));

        Assert.Equal("ops-team", plugin.ConsumerGroupPrefix());
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void A_blank_prefix_falls_back_rather_than_naming_a_group_after_nothing(string configured)
    {
        var plugin = new BowireKafkaProtocol();
        plugin.Initialize(new FakePluginSettings(("consumerGroupPrefix", configured)));

        Assert.Equal("bowire", plugin.ConsumerGroupPrefix());
    }

    [Fact]
    public void A_prefix_with_stray_whitespace_is_trimmed()
    {
        var plugin = new BowireKafkaProtocol();
        plugin.Initialize(new FakePluginSettings(("consumerGroupPrefix", "  ops-team  ")));

        Assert.Equal("ops-team", plugin.ConsumerGroupPrefix());
    }

    [Theory]
    [InlineData("not-a-number")]
    [InlineData("")]
    public void An_unparseable_timeout_leaves_the_default_rather_than_zero(string configured)
    {
        var plugin = new BowireKafkaProtocol();
        plugin.Initialize(new FakePluginSettings(("discoveryTimeoutSeconds", configured)));

        // A zero here would make every discovery time out instantly.
        Assert.Equal(TimeSpan.FromSeconds(5), plugin.DiscoveryTimeout());
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }
}
