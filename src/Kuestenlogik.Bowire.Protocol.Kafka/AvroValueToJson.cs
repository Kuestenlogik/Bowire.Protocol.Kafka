// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using System.Text.Json;
using Avro.Generic;

namespace Kuestenlogik.Bowire.Protocol.Kafka;

/// <summary>
/// Converts a <see cref="GenericRecord"/> (or any Avro generic value
/// returned by <c>GenericDatumReader</c>) into the matching
/// <see cref="System.Text.Json"/> shape and serialises it. Records →
/// objects, arrays → arrays, maps → objects, enums → strings,
/// fixed/bytes → base64. Numeric-logical-types (decimal / date /
/// timestamp) keep their canonical text representation so the workbench
/// surfaces them readably.
/// </summary>
internal static class AvroValueToJson
{
    private static readonly JsonSerializerOptions s_options = new()
    {
        WriteIndented = false,
    };

    public static string Serialize(object? avroValue)
    {
        var node = ToJsonNode(avroValue);
        return JsonSerializer.Serialize(node, s_options);
    }

    private static object? ToJsonNode(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case GenericRecord record:
                return RecordToObject(record);
            case GenericEnum e:
                return e.Value;
            case GenericFixed f:
                return Convert.ToBase64String(f.Value);
            case byte[] bytes:
                return Convert.ToBase64String(bytes);
            case string s:
                return s;
            case bool b:
                return b;
            case int i:
                return i;
            case long l:
                return l;
            case float f:
                return (double)f;
            case double d:
                return d;
            case System.Collections.IList list when value is not string:
                {
                    var arr = new List<object?>();
                    foreach (var item in list) arr.Add(ToJsonNode(item));
                    return arr;
                }
            case System.Collections.IDictionary dict:
                {
                    var obj = new Dictionary<string, object?>(StringComparer.Ordinal);
                    foreach (System.Collections.DictionaryEntry entry in dict)
                    {
                        var key = entry.Key?.ToString() ?? string.Empty;
                        obj[key] = ToJsonNode(entry.Value);
                    }
                    return obj;
                }
            default:
                // DateTime / decimal / TimeSpan from logical-type
                // conversions all stringify cleanly via InvariantCulture.
                return Convert.ToString(value, CultureInfo.InvariantCulture);
        }
    }

    private static Dictionary<string, object?> RecordToObject(GenericRecord record)
    {
        var obj = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var field in record.Schema.Fields)
        {
            if (record.TryGetValue(field.Name, out var value))
            {
                obj[field.Name] = ToJsonNode(value);
            }
        }
        return obj;
    }
}
