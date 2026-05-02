// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;

namespace Kuestenlogik.Bowire.Protocol.Kafka;

/// <summary>
/// URL parser for Bowire's Kafka plugin. Accepts both the
/// ecosystem-standard <c>kafka://</c> scheme and plain
/// <c>host:port</c> CSV so users can paste either a single broker
/// address or a comma-separated bootstrap-servers list straight
/// from their Kafka config.
/// </summary>
internal static class KafkaConnection
{
    /// <summary>Default Kafka broker port when the URL doesn't specify one.</summary>
    public const int DefaultPort = 9092;

    /// <summary>
    /// Parsed broker coordinates: comma-joined bootstrap servers, optional
    /// Schema Registry URL pulled from a <c>?schema-registry=…</c> query
    /// parameter on the input URL.
    /// </summary>
    public readonly record struct Endpoint(string BootstrapServers, string? SchemaRegistryUrl);

    /// <summary>
    /// Parse <paramref name="serverUrl"/> as
    /// <c>kafka://host:port[,host2:port2,...][?schema-registry=&lt;url&gt;]</c>
    /// (or the bare <c>host:port</c> form) and return the canonical
    /// bootstrap-servers CSV that Confluent.Kafka's
    /// <c>BootstrapServers</c> field expects, plus the optional Schema
    /// Registry URL. Returns <c>null</c> when the URL doesn't look like
    /// a Kafka address at all.
    /// </summary>
    public static Endpoint? TryParse(string? serverUrl)
    {
        if (string.IsNullOrWhiteSpace(serverUrl)) return null;

        var trimmed = serverUrl.TrimStart();
        if (trimmed.StartsWith("kafka://", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed["kafka://".Length..];
        else if (trimmed.Contains("://", StringComparison.Ordinal))
            return null; // some other scheme — not Kafka.

        // Pull the query string off before splitting hosts so a comma in a
        // future query value can't confuse the bootstrap-servers parser.
        string? schemaRegistryUrl = null;
        var queryIdx = trimmed.IndexOf('?', StringComparison.Ordinal);
        if (queryIdx >= 0)
        {
            schemaRegistryUrl = ExtractSchemaRegistry(trimmed[(queryIdx + 1)..]);
            trimmed = trimmed[..queryIdx];
        }

        trimmed = trimmed.TrimEnd('/');
        if (string.IsNullOrEmpty(trimmed)) return null;

        // Normalise each entry: ensure host:port, default port 9092.
        // Empty entries (double comma) are dropped silently so a
        // trailing comma in the URL doesn't break the parser.
        var parts = trimmed.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0) return null;

        var normalised = new List<string>(parts.Length);
        foreach (var part in parts)
        {
            var host = part;
            var port = DefaultPort;
            var colon = part.LastIndexOf(':');
            if (colon > 0)
            {
                host = part[..colon];
                var portStr = part[(colon + 1)..];
                if (int.TryParse(portStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                    port = parsed;
            }
            if (string.IsNullOrEmpty(host)) continue;
            normalised.Add(host + ":" + port.ToString(CultureInfo.InvariantCulture));
        }
        if (normalised.Count == 0) return null;

        return new Endpoint(string.Join(",", normalised), schemaRegistryUrl);
    }

    /// <summary>
    /// Pluck the <c>schema-registry</c> entry out of the query-string
    /// portion of the URL. Bowire doesn't try to be a general-purpose
    /// query parser — only the documented keys it understands.
    /// </summary>
    private static string? ExtractSchemaRegistry(string query)
    {
        if (string.IsNullOrEmpty(query)) return null;
        foreach (var pair in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pair.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var key = pair[..eq];
            var value = pair[(eq + 1)..];
            if (string.Equals(key, "schema-registry", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(key, "sr", StringComparison.OrdinalIgnoreCase))
            {
                return Uri.UnescapeDataString(value);
            }
        }
        return null;
    }
}
