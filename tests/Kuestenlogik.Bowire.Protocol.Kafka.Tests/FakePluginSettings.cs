// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Plugins;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// A workspace settings store holding whatever the test put in it, and
/// the minimal <see cref="IServiceProvider"/> that hands it to a plugin's
/// <c>Initialize</c> — the same shape the host uses.
/// </summary>
internal sealed class FakePluginSettings : IBowirePluginSettings, IServiceProvider
{
    private readonly Dictionary<string, string> _values;

    public FakePluginSettings(params (string Key, string Value)[] values)
        => _values = values.ToDictionary(v => v.Key, v => v.Value, StringComparer.Ordinal);

    public string? GetValue(string pluginId, string key)
        => _values.TryGetValue(key, out var value) ? value : null;

    public object? GetService(Type serviceType)
        => serviceType == typeof(IBowirePluginSettings) ? this : null;
}
