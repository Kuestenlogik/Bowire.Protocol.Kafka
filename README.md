# Bowire.Protocol.Kafka

Apache Kafka protocol plugin for the [Bowire](https://github.com/Kuestenlogik/Bowire) workbench. Browse topics, consume live traffic, produce test messages, and replay recorded Kafka streams through the Bowire mock server. Built on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet).

## URL shapes

```
kafka://broker.example:9092                                   # single broker
kafka://b1:9092,b2:9092,b3:9092                               # bootstrap servers CSV
broker.example:9094                                           # bare host:port also accepted
kafka://broker:9092?schema-registry=http://sr:8081            # with Confluent Schema Registry
kafka://broker:9092?sr=http%3A%2F%2Fsr%3A8081                 # short alias + URL-encoded
```

## Discovery

A short-lived `IAdminClient` queries broker metadata. Every non-internal topic surfaces as a service with two methods:

| Method | Kind | Description |
|--------|------|-------------|
| `consume` | ServerStreaming | Subscribe with a throwaway group id (`bowire-<hex>`), yield one envelope per message. |
| `produce` | Unary | Publish one message; optional `key` and `partition` via metadata. |

A synthetic `Cluster` service surfaces broker / topic counts so the sidebar shows "what's there" even before you pick a topic. Internal topics (`__consumer_offsets`, `_schemas`, `_confluent*`) are hidden unless the workbench's internal-services toggle is on.

## Consume envelope

Each message arrives as:

```json
{
  "topic": "orders",
  "partition": 0,
  "offset": 1234,
  "timestamp": 1745500800123,
  "key": "order-42",
  "keyBase64": "b3JkZXItNDI=",
  "value": "{\"id\":42,\"total\":19.95}",
  "valueBase64": "eyJpZCI6NDIsInRvdGFsIjoxOS45NX0=",
  "encoding": "avro",
  "bytes": 23
}
```

`key` / `value` are the UTF-8 decode when the bytes map cleanly; `null` for binary payloads. `keyBase64` / `valueBase64` are always present so the workbench can hex-dump arbitrary payloads (Avro, Protobuf, MessagePack, …). The `encoding` field is always present (nullable) — it's set when the body was decoded through the schema registry, otherwise `null`.

When the URL carries `?schema-registry=…` and the message body is framed in the Confluent wire format (`0x00` magic byte + 4-byte big-endian schema id + payload), the plugin decodes it on the fly and the `value` field carries the JSON projection. Coverage at v0:

| Confluent schema type | Decoded? | `encoding` tag |
|-----------------------|----------|----------------|
| Avro                  | Yes      | `"avro"`       |
| JSON Schema           | Yes (raw text — same path as Avro) | `"avro"` (v0 — JSON Schema-specific tagging is a follow-up) |
| Protobuf              | No (falls back to base64) | not set |

Plain UTF-8 / opaque binary stays in the original fallback path — the registry isn't consulted unless the framing prefix is present.

## Security

Two markers feed the Kafka security knobs from Bowire's environment auth helpers:

| Marker | Source | Effect |
|--------|--------|--------|
| `__bowireMtls__` | shared `mtls` auth helper across REST / gRPC / WebSocket / SignalR / Kafka | `SecurityProtocol = Ssl`, `SslCertificatePem` / `SslKeyPem` / `SslCaPem` populated from PEM strings (no temp files), optional `SslKeyPassword`. `allowSelfSigned` flips `EnableSslCertificateVerification = false`. |
| `__bowireKafkaSasl__` | Kafka-specific JSON `{ mechanism, username, password }`. Mechanism strings are uppercase with dashes: `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, `"OAUTHBEARER"` (matches the Confluent canonical form). | Maps to `SaslMechanism.Plain` / `ScramSha256` / `ScramSha512` / `OAuthBearer`. Combined with `__bowireMtls__` → `SecurityProtocol = SaslSsl`; alone → `SaslPlaintext`. |

Both markers are stripped from the metadata dict before it's forwarded as Kafka message headers, so secrets never reach the wire.

## Produce

The `produce` method publishes a single message taken from the first invocation payload:

| Metadata key | Purpose |
|--------------|---------|
| `key` | Message key (UTF-8 string) |
| `partition` | Target partition number (optional — round-robins when absent) |

## Mock replay

`KafkaMockEmitter` plugs into Bowire's mock server via `IBowireMockEmitter`. Recordings tagged `protocol: "kafka"` get re-published at the original cadence:

| Metadata key (first step) | Purpose | Default |
|---------------------------|---------|---------|
| `bootstrap` (or `bootstrap-servers`) | Broker CSV | `localhost:9092` |
| Per-step `key` / `partition` | Same knobs as the live produce path | — |

Payload source: `responseBinary` (base64) is preferred so Avro / Protobuf round-trips byte-for-byte; text-only recordings fall back to UTF-8 encoding of `body`.

## Install

```
dotnet tool install -g Kuestenlogik.Bowire.Tool
bowire plugin install Kuestenlogik.Bowire.Protocol.Kafka
```

Then start the workbench and enter a Kafka broker URL in the sidebar.

## Sample

A runnable end-to-end sample lives under [`samples/Kuestenlogik.Bowire.Protocol.Kafka.Sample`](samples/Kuestenlogik.Bowire.Protocol.Kafka.Sample) — a single-node Kafka cluster (Docker Compose) plus an ASP.NET Core host that publishes harbour-domain port-call and berth-status events on two topics, so the live consume stream is never quiet.

```bash
cd samples/Kuestenlogik.Bowire.Protocol.Kafka.Sample
docker compose up -d
dotnet run --project .
```

Then open <http://localhost:5080/bowire>, pick the **Kafka** tab, point it at `kafka://localhost:9092`, and stream `harbor.port-calls → consume`.

## Tests

The unit-test suite is hermetic and runs anywhere `dotnet test` does:

```
dotnet test --filter "Category!=Docker"
```

Real-broker round-trips (`KafkaRoundTripE2ETests`, `KafkaSchemaRegistryE2ETests`) sit behind the `Docker` xUnit trait. They use Testcontainers to spin up Confluent Kafka 7.7 (KRaft mode, no Zookeeper) and Schema Registry 7.7 in a shared bridge network, then drive the plugin against the live processes:

```
docker info       # confirm Docker Desktop / engine is reachable
dotnet test       # all tests, Docker ones included
```

## Relationship to `Bowire.Protocol.Storm`

This plugin is the generic Kafka baseline: schema-registry-agnostic, serializer-agnostic, usable against any cluster. The [`Bowire.Protocol.Storm`](https://github.com/Kuestenlogik/Bowire.Protocol.Storm) plugin layers Storm-specific conventions (schema-registry URL resolution, typed payload decoders, Storm topic-naming awareness) on top for users inside the Storm ecosystem. Run either alone or install both side by side — each registers under its own protocol id (`kafka` vs `storm`).
