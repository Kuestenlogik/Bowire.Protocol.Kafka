# Sample — Kafka tap

A small ASP.NET Core host that boots a Bowire endpoint and pumps a
steady stream of harbour-domain events into a single-node Kafka
cluster. Two topics keep the workbench's stream pane lively:

| Topic | Cadence | Payload |
|-------|---------|---------|
| `harbor.port-calls`   | ~2 s | `PortCallEvent` cycling through `arriving` / `docked` / `unloading` / `departing` |
| `harbor.berth-status` | ~5 s | `BerthStatusEvent` snapshot of berth occupancy |

Both topics are auto-created on startup, so there's nothing to
pre-provision. Messages are JSON-encoded — no Schema Registry needed
to keep the start-up to a single `docker compose up`.

## Run

1. **Boot Kafka** (single-node KRaft, no Zookeeper):

   ```sh
   docker compose up -d
   ```

   Brings up `confluentinc/cp-kafka:7.7.1` on `localhost:9092` plus a
   tiny [kafka-ui](https://github.com/provectus/kafka-ui) on
   <http://localhost:8080> in case you want to eyeball the topics.

2. **Run the sample** (from the repo root):

   ```sh
   dotnet run --project samples/Kuestenlogik.Bowire.Protocol.Kafka.Sample
   ```

3. **Watch in Bowire** — open <http://localhost:5080/bowire> (or run
   `bowire` from the CLI), pick the **Kafka** tab, point it at
   `kafka://localhost:9092`, and stream
   `harbor.port-calls → consume`. Switch to `harbor.berth-status` for
   the chunkier 5 s snapshot stream.

## Tear down

```sh
docker compose down -v
```

The `-v` drops the KRaft metadata volume, so the next `up` starts
clean.

## Layout

| File | Role |
|------|------|
| `Program.cs`                     | Minimal-host bootstrap: `AddBowire()` + `MapBowire()` + register the harbour publisher hosted service. |
| `Harbor/HarborEvents.cs`         | Two record types — `PortCallEvent` and `BerthStatusEvent`. |
| `Harbor/HarborStreamPublisher.cs`| `BackgroundService` that auto-creates the topics, then publishes on a 2 s / 5 s cadence. |
| `appsettings.json`               | `Kafka:BootstrapServers` + topic names. Override at the command line if you point at a remote broker. |
| `docker-compose.yml`             | Single-node Kafka cluster + Kafka UI. |

The sample lives in the same repo as the plugin, so contributors can
`dotnet run` it without cloning anything else.
