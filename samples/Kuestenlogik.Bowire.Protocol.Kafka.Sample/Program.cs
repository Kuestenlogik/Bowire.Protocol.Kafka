// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using Kuestenlogik.Bowire;
using Kuestenlogik.Bowire.Protocol.Kafka.Sample.Harbor;
// Force the Kafka plugin assembly to load before AddBowire's
// reflection scan runs — without an explicit type reference the
// JIT only loads the plugin DLL on first use, too late for the
// discovery pass.
_ = typeof(Kuestenlogik.Bowire.Protocol.Kafka.BowireKafkaProtocol);

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
// Pre-seed the Kafka broker (matches docker-compose.yml) as a
// discovered ServerUrl so the workbench shows the topics in the
// sidebar the moment the page loads.
app.MapBowire(options =>
{
    options.ServerUrls.Add("kafka://localhost:9092");
});

app.Run();
