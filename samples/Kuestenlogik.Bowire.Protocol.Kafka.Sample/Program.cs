// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Protocol.Kafka.Sample.Harbor;

// Runnable Bowire-Kafka demo. Hosts a small ASP.NET Core app with
// AddBowire() so the workbench attaches at /bowire, then publishes a
// steady stream of harbour-domain events into a single-node Kafka
// cluster:
//
//   harbor.port-calls   ── PortCallEvent every ~2 s (cycling phase)
//   harbor.berth-status ── BerthStatusEvent every ~5 s (occupancy snapshot)
//
// Topics auto-create on startup. Bring up Kafka with
// `docker compose up -d` from this directory, then `dotnet run` and
// open http://localhost:5080/bowire — pick the Kafka tab and stream
// `harbor.port-calls → consume`.

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddBowire();
builder.Services.AddHarborStreamPublisher(builder.Configuration);

var app = builder.Build();
app.MapBowire();

app.Run();
