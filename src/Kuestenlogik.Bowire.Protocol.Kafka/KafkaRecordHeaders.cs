// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using Confluent.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka;

/// <summary>
/// The record headers of a produce, and the headers a consumed record
/// carries (#93). Produce used to drop every metadata entry but <c>key</c>
/// and <c>partition</c>, though its comments said the rest went out as
/// headers.
/// </summary>
internal static class KafkaRecordHeaders
{
    /// <summary>Metadata the plugin reads itself; it addresses the record and is not a header.</summary>
    private static readonly HashSet<string> s_consumedByProduce = new(StringComparer.Ordinal) { "key", "partition" };

    /// <summary>
    /// The headers for a produce: every metadata entry that is neither a
    /// Bowire marker (<c>__bowire*</c>, which can hold secrets) nor one the
    /// plugin consumes. <c>null</c> when there are none.
    /// </summary>
    public static Headers? FromMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null) return null;
        Headers? headers = null;
        foreach (var (name, value) in BowireMetadataKeys.WireHeaders(metadata))
        {
            if (s_consumedByProduce.Contains(name)) continue;
            headers ??= [];
            headers.Add(name, Encoding.UTF8.GetBytes(value ?? ""));
        }
        return headers;
    }

    /// <summary>
    /// The headers of a consumed record, in order and with repeats, as
    /// <c>{ key, value, valueBase64 }</c>: <c>value</c> is the UTF-8 text,
    /// or <c>null</c> for bytes that are not; <c>valueBase64</c> always holds
    /// the bytes. <c>null</c> when the record has none.
    /// </summary>
    public static IReadOnlyList<object>? ToEnvelope(Headers? headers)
    {
        if (headers is null || headers.Count == 0) return null;
        var list = new List<object>(headers.Count);
        foreach (var header in headers)
        {
            var bytes = header.GetValueBytes() ?? [];
            list.Add(new
            {
                key = header.Key,
                value = BowireKafkaProtocol.TryDecodeUtf8(bytes),
                valueBase64 = Convert.ToBase64String(bytes),
            });
        }
        return list;
    }
}
