// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Models;

namespace Kuestenlogik.Bowire.Protocol.Kafka;

/// <summary>
/// Bowire protocol plugin for Apache Kafka. Given a broker URL
/// (<c>kafka://host:port[,host2:port2,...]</c>) the plugin enumerates
/// topics via the Admin API, surfaces each topic as a Bowire service
/// with two methods (<c>consume</c> streaming, <c>produce</c> unary),
/// and decodes every consumed message into a JSON envelope so the
/// workbench can render it alongside every other streaming protocol.
/// </summary>
/// <remarks>
/// <para>
/// Discovery: a short-lived <see cref="IAdminClient"/> queries broker
/// metadata. Internal topics (<c>__consumer_offsets</c>, schema
/// registry topics) are filtered out unless <c>showInternalServices</c>
/// is on, matching the other plugins' semantics.
/// </para>
/// <para>
/// Consume: opens a consumer with a generated group id
/// (<c>bowire-&lt;guid&gt;</c>), subscribes to the topic, and yields
/// a JSON envelope per message. The envelope carries
/// <c>topic</c>, <c>partition</c>, <c>offset</c>, <c>timestamp</c>,
/// <c>key</c> / <c>keyBase64</c>, <c>value</c> / <c>valueBase64</c>,
/// and <c>bytes</c>. Text fields round-trip via UTF-8 when the payload
/// decodes cleanly; otherwise they fall back to base64.
/// </para>
/// <para>
/// Produce: single <see cref="IProducer{TKey,TValue}"/> instance per
/// invoke, publishes one message taken from the first <c>jsonMessages</c>
/// entry. Metadata keys <c>key</c> and <c>partition</c> tune the message
/// coordinates.
/// </para>
/// </remarks>
public sealed class BowireKafkaProtocol : IBowireProtocol
{
    /// <summary>Method name of the streaming consume operation.</summary>
    public const string ConsumeMethodName = "consume";

    /// <summary>Method name of the unary produce operation.</summary>
    public const string ProduceMethodName = "produce";

    /// <summary>Synthetic service for the broker itself (metadata / cluster info).</summary>
    public const string ClusterServiceName = "Cluster";

    /// <inheritdoc />
    public string Name => "Kafka";

    /// <inheritdoc />
    public string Id => "kafka";

    /// <inheritdoc />
    // Three horizontal "topic" bars + partition dots, muted blue —
    // mirrors Confluent's visual idiom without using their logo.
    public string IconSvg => """<svg viewBox="0 0 24 24" fill="none" stroke="#60a5fa" stroke-width="1.5" width="16" height="16" aria-hidden="true"><path d="M4 6h12"/><path d="M4 12h16"/><path d="M4 18h10"/><circle cx="19" cy="6" r="1.2" fill="#60a5fa"/><circle cx="22" cy="12" r="1.2" fill="#60a5fa"/><circle cx="17" cy="18" r="1.2" fill="#60a5fa"/></svg>""";

    /// <inheritdoc />
    public IReadOnlyList<BowirePluginSetting> Settings =>
    [
        new("discoveryTimeoutSeconds", "Discovery timeout",
            "Max seconds to wait on broker metadata during discovery",
            "number", 5),
        new("consumerGroupPrefix", "Consumer-group prefix",
            "Prefix used when generating a throwaway group id for streaming consume",
            "string", "bowire"),
    ];

    /// <inheritdoc />
    public async Task<List<BowireServiceInfo>> DiscoverAsync(
        string serverUrl, bool showInternalServices, CancellationToken ct = default)
    {
        var endpoint = KafkaConnection.TryParse(serverUrl);
        if (endpoint is null) return [];

        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = endpoint.Value.BootstrapServers,
            SocketTimeoutMs = 5_000,
        };
        // Discovery doesn't carry metadata yet (see InvokeAsync override
        // for the path that does), but the security wiring is set up the
        // same way so the Storm/Kafka roadmap migration can drop a
        // metadata-aware Discover overload in without re-plumbing.

        using var admin = new AdminClientBuilder(adminConfig).Build();

        Confluent.Kafka.Metadata metadata;
        try
        {
            // Explicitly pass the cancellation token's deadline as a
            // TimeSpan — librdkafka's GetMetadata is synchronous and
            // ignores the token directly, but its timeout parameter
            // keeps us responsive.
            metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
        }
        catch (KafkaException)
        {
            // Broker unreachable / connection refused — not an error
            // for discovery, just means there's nothing here.
            return [];
        }

        // Pull schemas eagerly during discovery so the sidebar surfaces
        // "topic X has Avro schema Y" without a per-message round-trip.
        // Failure here is non-fatal — schemaless topics keep working.
        KafkaSchemaRegistry? registry = null;
        Dictionary<string, (string? KeyDescription, string? ValueDescription)> schemaSummaries = new(StringComparer.Ordinal);
        if (endpoint.Value.SchemaRegistryUrl is { Length: > 0 } srUrl)
        {
            try
            {
                registry = new KafkaSchemaRegistry(srUrl);
                foreach (var topic in metadata.Topics)
                {
                    if (!showInternalServices && IsInternalTopic(topic.Topic)) continue;
                    var keySchema = await registry.TryGetLatestAsync(topic.Topic + "-key").ConfigureAwait(false);
                    var valueSchema = await registry.TryGetLatestAsync(topic.Topic + "-value").ConfigureAwait(false);
                    schemaSummaries[topic.Topic] = (
                        keySchema is null ? null : keySchema.SchemaType + " v" + keySchema.Version,
                        valueSchema is null ? null : valueSchema.SchemaType + " v" + valueSchema.Version);
                }
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                // Registry unreachable — keep going schemaless.
            }
            finally
            {
                registry?.Dispose();
            }
        }

        var services = new List<BowireServiceInfo>
        {
            BuildClusterService(serverUrl, endpoint.Value, metadata),
        };

        foreach (var topic in metadata.Topics.OrderBy(t => t.Topic, StringComparer.Ordinal))
        {
            if (!showInternalServices && IsInternalTopic(topic.Topic)) continue;
            schemaSummaries.TryGetValue(topic.Topic, out var schema);
            services.Add(BuildTopicService(serverUrl, topic, schema));
        }

        return services;
    }

    /// <inheritdoc />
    public async Task<InvokeResult> InvokeAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null, CancellationToken ct = default)
    {
        var endpoint = KafkaConnection.TryParse(serverUrl);
        if (endpoint is null)
        {
            return new InvokeResult(
                null, 0, "Invalid Kafka broker URL.",
                new Dictionary<string, string>());
        }

        if (!string.Equals(method, ProduceMethodName, StringComparison.OrdinalIgnoreCase))
        {
            return new InvokeResult(
                null, 0,
                "Kafka invocation only supports 'produce'. Open the 'consume' stream to observe topic traffic.",
                new Dictionary<string, string>());
        }

        var payload = jsonMessages.FirstOrDefault() ?? "{}";
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = endpoint.Value.BootstrapServers,
            Acks = Acks.Leader,
        };

        // Apply mTLS / SASL knobs from the magic metadata markers and
        // strip them before forwarding the rest as Kafka headers.
        var sanitisedMetadata = KafkaSecurityConfig.Apply(producerConfig, metadata);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var producer = new ProducerBuilder<string?, byte[]>(producerConfig).Build();

        var key = sanitisedMetadata?.TryGetValue("key", out var k) == true ? k : null;
        var message = new Message<string?, byte[]>
        {
            Key = key,
            Value = Encoding.UTF8.GetBytes(payload),
        };

        DeliveryResult<string?, byte[]> result;
        try
        {
            if (sanitisedMetadata?.TryGetValue("partition", out var partitionStr) == true &&
                int.TryParse(partitionStr, out var partition))
            {
                var topicPartition = new TopicPartition(service, new Partition(partition));
                result = await producer.ProduceAsync(topicPartition, message, ct);
            }
            else
            {
                result = await producer.ProduceAsync(service, message, ct);
            }
        }
        catch (ProduceException<string?, byte[]> ex)
        {
            sw.Stop();
            return new InvokeResult(
                null, sw.ElapsedMilliseconds,
                "Error: " + ex.Error.Reason,
                new Dictionary<string, string>());
        }
        sw.Stop();

        var responseMeta = new Dictionary<string, string>
        {
            ["topic"] = result.Topic,
            ["partition"] = result.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["offset"] = result.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        return new InvokeResult(
            JsonSerializer.Serialize(new
            {
                topic = result.Topic,
                partition = result.Partition.Value,
                offset = result.Offset.Value,
                bytes = message.Value?.Length ?? 0,
            }),
            sw.ElapsedMilliseconds,
            "OK",
            responseMeta);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> InvokeStreamAsync(
        string serverUrl, string service, string method,
        List<string> jsonMessages, bool showInternalServices,
        Dictionary<string, string>? metadata = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var endpoint = KafkaConnection.TryParse(serverUrl);
        if (endpoint is null) yield break;

        if (!string.Equals(method, ConsumeMethodName, StringComparison.OrdinalIgnoreCase))
            yield break;

        var groupId = "bowire-" + Guid.NewGuid().ToString("N")[..12];
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = endpoint.Value.BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
            SocketTimeoutMs = 5_000,
        };
        KafkaSecurityConfig.Apply(consumerConfig, metadata);

        using var consumer = new ConsumerBuilder<byte[]?, byte[]>(consumerConfig).Build();
        consumer.Subscribe(service);

        // Optional Schema Registry — held open for the duration of the
        // stream so per-message decode pays the HTTP cost only once per
        // schema id (the registry client caches internally).
        using var registry = endpoint.Value.SchemaRegistryUrl is { Length: > 0 } srUrl
            ? new KafkaSchemaRegistry(srUrl)
            : null;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<byte[]?, byte[]>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                }
                catch (ConsumeException)
                {
                    yield break;
                }

                if (result is null || result.Message is null) continue;
                yield return await BuildEnvelopeAsync(result, registry).ConfigureAwait(false);
            }
        }
        finally
        {
            try { consumer.Close(); } catch { /* best-effort */ }
        }
    }

    /// <inheritdoc />
    public Task<IBowireChannel?> OpenChannelAsync(
        string serverUrl, string service, string method,
        bool showInternalServices, Dictionary<string, string>? metadata = null,
        CancellationToken ct = default)
        => Task.FromResult<IBowireChannel?>(null);

    private static BowireServiceInfo BuildClusterService(
        string serverUrl, KafkaConnection.Endpoint endpoint, Confluent.Kafka.Metadata metadata)
    {
        var description =
            $"Kafka cluster on {endpoint.BootstrapServers}. " +
            $"{metadata.Brokers.Count} broker(s), {metadata.Topics.Count} topic(s).";
        // Cluster service has no methods — it's purely descriptive so
        // the user sees broker count, partition totals, etc. in the
        // sidebar without a no-op method.
        return new BowireServiceInfo(ClusterServiceName, "kafka", [])
        {
            Source = "kafka",
            OriginUrl = serverUrl,
            Description = description,
        };
    }

    private static BowireServiceInfo BuildTopicService(
        string serverUrl, Confluent.Kafka.TopicMetadata topic,
        (string? KeyDescription, string? ValueDescription) schema = default)
    {
        var description = topic.Error.IsError
            ? "Topic '" + topic.Topic + "' — error: " + topic.Error.Reason
            : "Topic '" + topic.Topic + "' with " + topic.Partitions.Count +
              " partition(s).";

        // Schema Registry signal — when discovery resolved a key/value
        // schema, append it to the description so the sidebar surfaces
        // it. Schemaless topics get the same description as before.
        if (schema.KeyDescription is not null || schema.ValueDescription is not null)
        {
            description += " key=" + (schema.KeyDescription ?? "—")
                + ", value=" + (schema.ValueDescription ?? "—");
        }

        var methods = new List<BowireMethodInfo>
        {
            BuildConsumeMethod(topic.Topic, description),
            BuildProduceMethod(topic.Topic),
        };
        return new BowireServiceInfo(topic.Topic, "kafka", methods)
        {
            Source = "kafka",
            OriginUrl = serverUrl,
            Description = description,
        };
    }

    private static BowireMethodInfo BuildConsumeMethod(string topic, string description) =>
        new(
            Name: ConsumeMethodName,
            FullName: $"kafka/{topic}/{ConsumeMethodName}",
            ClientStreaming: false,
            ServerStreaming: true,
            InputType: new BowireMessageInfo("KafkaConsumeRequest", "kafka.ConsumeRequest", []),
            OutputType: BuildConsumeOutput(),
            MethodType: "ServerStreaming")
        {
            Summary = "Consume " + topic,
            Description = description,
        };

    private static BowireMethodInfo BuildProduceMethod(string topic) =>
        new(
            Name: ProduceMethodName,
            FullName: $"kafka/{topic}/{ProduceMethodName}",
            ClientStreaming: false,
            ServerStreaming: false,
            InputType: BuildProduceInput(),
            OutputType: BuildProduceOutput(),
            MethodType: "Unary")
        {
            Summary = "Produce to " + topic,
            Description = "Publish a single message to topic '" + topic +
                          "'. Set message key and target partition via metadata keys 'key' and 'partition'.",
        };

    private static BowireMessageInfo BuildProduceInput() => new(
        "KafkaProduceRequest", "kafka.ProduceRequest",
        [
            new BowireFieldInfo("value", 1, "string", "LABEL_OPTIONAL", false, false, null, null)
            {
                Description = "Message value (UTF-8 text; binary payloads accepted as-is)",
                Required = true,
            },
        ]);

    private static BowireMessageInfo BuildProduceOutput() => new(
        "KafkaProduceResponse", "kafka.ProduceResponse",
        [
            new BowireFieldInfo("topic", 1, "string", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("partition", 2, "int32", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("offset", 3, "int64", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("bytes", 4, "int32", "LABEL_OPTIONAL", false, false, null, null),
        ]);

    private static BowireMessageInfo BuildConsumeOutput() => new(
        "KafkaMessage", "kafka.Message",
        [
            new BowireFieldInfo("topic", 1, "string", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("partition", 2, "int32", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("offset", 3, "int64", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("timestamp", 4, "int64", "LABEL_OPTIONAL", false, false, null, null)
            {
                Description = "Unix-ms timestamp of the message (broker-assigned or producer-set).",
            },
            new BowireFieldInfo("key", 5, "string", "LABEL_OPTIONAL", false, false, null, null)
            {
                Description = "UTF-8 decoded key when decodable; null + keyBase64 otherwise.",
            },
            new BowireFieldInfo("keyBase64", 6, "string", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("value", 7, "string", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("valueBase64", 8, "string", "LABEL_OPTIONAL", false, false, null, null),
            new BowireFieldInfo("bytes", 9, "int32", "LABEL_OPTIONAL", false, false, null, null),
        ]);

    /// <summary>
    /// Render a consumed message as a Bowire stream envelope. Keys
    /// and values are surfaced as UTF-8 text whenever they decode
    /// cleanly; binary payloads only populate the base64 field so the
    /// workbench can hex-dump them without guessing at the shape.
    /// </summary>
    internal static string BuildEnvelope(ConsumeResult<byte[]?, byte[]> result)
    {
        return BuildEnvelopeAsync(result, registry: null).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Schema-Registry-aware envelope builder. When <paramref name="registry"/>
    /// is non-null and the message body is framed in the Confluent wire
    /// format (magic byte 0x00 + 4-byte schema id), the value is decoded
    /// to a JSON string + an <c>encoding</c> field tags the schema kind
    /// ("avro" / "json") so the UI can render the originally-typed shape
    /// instead of a base64 blob.
    /// </summary>
    internal static async Task<string> BuildEnvelopeAsync(
        ConsumeResult<byte[]?, byte[]> result,
        KafkaSchemaRegistry? registry)
    {
        var keyBytes = result.Message.Key;
        var valueBytes = result.Message.Value ?? Array.Empty<byte>();
        string? keyText = TryDecodeUtf8(keyBytes);
        string? valueText = TryDecodeUtf8(valueBytes);
        string? encoding = null;

        if (registry is not null)
        {
            var decoded = await registry.TryDecodeAsync(valueBytes).ConfigureAwait(false);
            if (decoded is not null)
            {
                valueText = decoded;
                encoding = "avro"; // JSON-Schema bodies decode the same way; tagged as Avro for the v0 slice
            }
        }

        var envelope = new
        {
            topic = result.Topic,
            partition = result.Partition.Value,
            offset = result.Offset.Value,
            timestamp = result.Message.Timestamp.UnixTimestampMs,
            key = keyText,
            keyBase64 = keyBytes is null ? null : Convert.ToBase64String(keyBytes),
            value = valueText,
            valueBase64 = Convert.ToBase64String(valueBytes),
            bytes = valueBytes.Length,
            encoding,
        };
        return JsonSerializer.Serialize(envelope);
    }

    /// <summary>
    /// Attempt a strict UTF-8 decode. Returns the string when every
    /// byte maps; null when any byte is invalid (binary payload). Null
    /// inputs (Kafka messages without a key) return null.
    /// </summary>
    internal static string? TryDecodeUtf8(byte[]? bytes)
    {
        if (bytes is null) return null;
        if (bytes.Length == 0) return string.Empty;
        var encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        try
        {
            return encoding.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>
    /// True for topics Kafka manages internally (consumer offsets,
    /// transaction coordinators, schema-registry backing topics).
    /// Mirrors the convention <c>kafka-topics --list</c>'s
    /// <c>--exclude-internal</c> flag uses.
    /// </summary>
    internal static bool IsInternalTopic(string topic) =>
        topic.StartsWith("__", StringComparison.Ordinal) ||
        topic.StartsWith("_schemas", StringComparison.Ordinal) ||
        topic.StartsWith("_confluent", StringComparison.Ordinal);
}
