// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Globalization;
using Confluent.Kafka;
using Kuestenlogik.Bowire.Mocking;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Bowire.Protocol.Kafka;

/// <summary>
/// Plugs into Bowire's mock server via the
/// <see cref="IBowireMockEmitter"/> extension point. When a recording
/// contains steps tagged <c>protocol: "kafka"</c>, the emitter opens a
/// single producer against the configured broker and re-publishes each
/// step's captured value at the original cadence derived from
/// <see cref="BowireRecordingStep.CapturedAt"/>.
/// </summary>
/// <remarks>
/// <para>
/// Destination is read from the first Kafka step's metadata:
/// </para>
/// <list type="bullet">
///   <item><c>bootstrap</c> (or <c>bootstrap-servers</c>): broker CSV
///   (default <c>localhost:9092</c>).</item>
///   <item>Target topic comes from
///   <see cref="BowireRecordingStep.Service"/> (one step per topic,
///   same shape the recorder writes).</item>
///   <item>Optional per-step <c>key</c> and <c>partition</c> metadata
///   tune individual message placement.</item>
/// </list>
/// <para>
/// Payload source: <see cref="BowireRecordingStep.ResponseBinary"/>
/// (base64 raw bytes) is preferred so Avro / Protobuf / arbitrary
/// binary messages round-trip byte-for-byte. Falls back to UTF-8
/// encoding of <see cref="BowireRecordingStep.Body"/> for text-only
/// recordings.
/// </para>
/// </remarks>
public sealed class KafkaMockEmitter : IBowireMockEmitter
{
    private IProducer<byte[]?, byte[]>? _producer;
    private CancellationTokenSource? _cts;
    private Task? _schedulerTask;
    private bool _disposed;

    /// <inheritdoc />
    public string Id => "kafka";

    /// <inheritdoc />
    public bool CanEmit(BowireRecording recording)
    {
        ArgumentNullException.ThrowIfNull(recording);
        return recording.Steps.Any(IsKafkaStep);
    }

    /// <inheritdoc />
    public Task StartAsync(
        BowireRecording recording,
        MockEmitterOptions options,
        ILogger logger,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(recording);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        var kafkaSteps = recording.Steps.Where(IsKafkaStep).ToList();
        if (kafkaSteps.Count == 0) return Task.CompletedTask;

        var bootstrap = ReadBootstrap(kafkaSteps[0]);
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.Leader,
        };
        _producer = new ProducerBuilder<byte[]?, byte[]>(producerConfig).Build();

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _schedulerTask = Task.Run(() => RunAsync(kafkaSteps, options, logger, _cts.Token), _cts.Token);

        logger.LogInformation(
            "kafka-emitter sending → {Bootstrap} (steps={Count})",
            bootstrap, kafkaSteps.Count);
        return Task.CompletedTask;
    }

    private static bool IsKafkaStep(BowireRecordingStep s) =>
        string.Equals(s.Protocol, "kafka", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Read the bootstrap-servers CSV from the step's metadata. Falls
    /// back to <c>localhost:9092</c> so a recording captured without
    /// metadata still does something on replay against a local dev
    /// broker.
    /// </summary>
    internal static string ReadBootstrap(BowireRecordingStep first)
    {
        if (first.Metadata is not null)
        {
            if (first.Metadata.TryGetValue("bootstrap", out var csv) && !string.IsNullOrWhiteSpace(csv))
                return csv;
            if (first.Metadata.TryGetValue("bootstrap-servers", out var csv2) && !string.IsNullOrWhiteSpace(csv2))
                return csv2;
        }
        return "localhost:9092";
    }

    private async Task RunAsync(
        List<BowireRecordingStep> steps,
        MockEmitterOptions options,
        ILogger logger,
        CancellationToken ct)
    {
        if (_producer is null) return;

        var baseCapturedAt = steps[0].CapturedAt;
        var speed = options.ReplaySpeed;

        do
        {
            var scheduleStartTicks = Environment.TickCount64;

            foreach (var step in steps)
            {
                ct.ThrowIfCancellationRequested();

                if (speed > 0)
                {
                    var targetOffsetMs = (long)((step.CapturedAt - baseCapturedAt) / speed);
                    var elapsed = Environment.TickCount64 - scheduleStartTicks;
                    var waitMs = targetOffsetMs - elapsed;
                    if (waitMs > 0)
                    {
                        try { await Task.Delay(TimeSpan.FromMilliseconds(waitMs), ct); }
                        catch (OperationCanceledException) { return; }
                    }
                }

                await EmitAsync(step, logger, ct);
            }
        }
        while (options.Loop && !ct.IsCancellationRequested);
    }

    private async Task EmitAsync(BowireRecordingStep step, ILogger logger, CancellationToken ct)
    {
        var payload = DecodePayload(step, logger);
        if (payload is null) return;

        if (string.IsNullOrEmpty(step.Service))
        {
            logger.LogWarning(
                "kafka-emitter skipping step '{StepId}': step.service (topic) is empty.", step.Id);
            return;
        }

        var key = step.Metadata?.TryGetValue("key", out var k) == true
            ? System.Text.Encoding.UTF8.GetBytes(k)
            : null;
        var message = new Message<byte[]?, byte[]> { Key = key, Value = payload };

        try
        {
            if (step.Metadata?.TryGetValue("partition", out var partitionStr) == true &&
                int.TryParse(partitionStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var partition))
            {
                await _producer!.ProduceAsync(
                    new TopicPartition(step.Service, new Partition(partition)), message, ct);
            }
            else
            {
                await _producer!.ProduceAsync(step.Service, message, ct);
            }
            logger.LogInformation(
                "kafka-emit(step={StepId}, topic={Topic}, bytes={Bytes})",
                step.Id, step.Service, payload.Length);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "kafka-emitter send failed for step '{StepId}' on topic '{Topic}'; scheduler continues.",
                step.Id, step.Service);
        }
    }

    /// <summary>
    /// Decode the step's payload to bytes. Same precedence as the DIS
    /// and UDP emitters: <see cref="BowireRecordingStep.ResponseBinary"/>
    /// (base64) wins; <see cref="BowireRecordingStep.Body"/> is
    /// UTF-8-encoded as a fallback.
    /// </summary>
    internal static byte[]? DecodePayload(BowireRecordingStep step, ILogger logger)
    {
        if (!string.IsNullOrEmpty(step.ResponseBinary))
        {
            try
            {
                return Convert.FromBase64String(step.ResponseBinary);
            }
            catch (FormatException ex)
            {
                logger.LogWarning(
                    "kafka-emitter skipping step '{StepId}': malformed base64 payload ({Message}).",
                    step.Id, ex.Message);
                return null;
            }
        }
        if (!string.IsNullOrEmpty(step.Body))
        {
            return System.Text.Encoding.UTF8.GetBytes(step.Body);
        }
        logger.LogWarning(
            "kafka-emitter skipping step '{StepId}': neither responseBinary nor body present.",
            step.Id);
        return null;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_cts is not null)
        {
            try { await _cts.CancelAsync(); }
            catch (ObjectDisposedException) { /* already torn down */ }
        }
        if (_schedulerTask is not null)
        {
            try { await _schedulerTask; }
            catch (OperationCanceledException) { /* expected */ }
            catch { /* scheduler cleanup is best-effort */ }
        }
        _producer?.Dispose();
        _cts?.Dispose();
    }
}
