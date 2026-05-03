// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

namespace Kuestenlogik.Bowire.Protocol.Kafka.Sample.Harbor;

/// <summary>
/// One port-call event — published every ~2 s on the
/// <c>harbor.port-calls</c> topic. The <c>kind</c> string cycles through
/// the lifecycle phases (<c>arriving</c>, <c>docked</c>,
/// <c>unloading</c>, <c>departing</c>) so the consume stream in Bowire
/// is never repetitive.
/// </summary>
public sealed record PortCallEvent(
    string ShipName,
    int Imo,
    string Berth,
    string Kind,
    int CargoTeu,
    DateTimeOffset At);

/// <summary>
/// Snapshot of the harbour's berth occupancy — published every ~5 s on
/// the <c>harbor.berth-status</c> topic. Gives the workbench a second,
/// chunkier stream to switch between for visual contrast.
/// </summary>
public sealed record BerthStatusEvent(
    string Berth,
    string State,
    string? CurrentShip,
    int QueueLength,
    DateTimeOffset At);
