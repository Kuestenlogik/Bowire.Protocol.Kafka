// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire.Mocking;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// Unit tests for <see cref="KafkaMockEmitter"/> that don't require a
/// live broker. The wire-level "does it actually publish?" path is
/// exercised via Testcontainers-Kafka in a follow-up integration
/// lane; the behaviours below cover the metadata / payload decoding
/// logic that would be the same regardless of broker reachability.
/// </summary>
public sealed class KafkaMockEmitterTests
{
    [Fact]
    public async Task CanEmit_TrueWhenRecordingHasKafkaStep()
    {
        await using var emitter = new KafkaMockEmitter();
        var rec = new BowireRecording
        {
            Steps =
            {
                new BowireRecordingStep { Protocol = "rest" },
                new BowireRecordingStep { Protocol = "kafka" }
            }
        };
        Assert.True(emitter.CanEmit(rec));
    }

    [Fact]
    public async Task CanEmit_FalseWhenRecordingHasNoKafkaStep()
    {
        await using var emitter = new KafkaMockEmitter();
        var rec = new BowireRecording
        {
            Steps = { new BowireRecordingStep { Protocol = "mqtt" } }
        };
        Assert.False(emitter.CanEmit(rec));
    }

    [Fact]
    public void ReadBootstrap_PrefersBootstrapMetadataKey()
    {
        var step = new BowireRecordingStep
        {
            Metadata = new Dictionary<string, string>
            {
                ["bootstrap"] = "b1:9092,b2:9092",
            }
        };
        Assert.Equal("b1:9092,b2:9092", KafkaMockEmitter.ReadBootstrap(step));
    }

    [Fact]
    public void ReadBootstrap_FallsBackToBootstrapServersKey()
    {
        var step = new BowireRecordingStep
        {
            Metadata = new Dictionary<string, string>
            {
                ["bootstrap-servers"] = "broker:9094",
            }
        };
        Assert.Equal("broker:9094", KafkaMockEmitter.ReadBootstrap(step));
    }

    [Fact]
    public void ReadBootstrap_DefaultsToLocalhost()
    {
        var step = new BowireRecordingStep();
        Assert.Equal("localhost:9092", KafkaMockEmitter.ReadBootstrap(step));
    }

    [Fact]
    public void DecodePayload_PrefersResponseBinary()
    {
        var step = new BowireRecordingStep
        {
            ResponseBinary = Convert.ToBase64String([0xDE, 0xAD]),
            Body = "ignored"
        };
        var bytes = KafkaMockEmitter.DecodePayload(step, NullLogger.Instance);
        Assert.Equal(new byte[] { 0xDE, 0xAD }, bytes);
    }

    [Fact]
    public void DecodePayload_FallsBackToBodyAsUtf8()
    {
        var step = new BowireRecordingStep { Body = "hello" };
        var bytes = KafkaMockEmitter.DecodePayload(step, NullLogger.Instance);
        Assert.Equal(new byte[] { 0x68, 0x65, 0x6C, 0x6C, 0x6F }, bytes);
    }

    [Fact]
    public void DecodePayload_Nothing_ReturnsNull()
    {
        Assert.Null(KafkaMockEmitter.DecodePayload(new BowireRecordingStep(), NullLogger.Instance));
    }

    [Fact]
    public void DecodePayload_MalformedBase64_ReturnsNull()
    {
        var step = new BowireRecordingStep { ResponseBinary = "not-base64!" };
        Assert.Null(KafkaMockEmitter.DecodePayload(step, NullLogger.Instance));
    }

    [Fact]
    public async Task Id_IsKafka()
    {
        await using var emitter = new KafkaMockEmitter();
        Assert.Equal("kafka", emitter.Id);
    }
}
