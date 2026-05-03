// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Sample.Harbor;

/// <summary>
/// Strongly-typed view of the <c>Kafka:</c> section of
/// <c>appsettings.json</c>.
/// </summary>
public sealed class KafkaSampleOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string PortCallsTopic { get; set; } = "harbor.port-calls";
    public string BerthStatusTopic { get; set; } = "harbor.berth-status";
}

/// <summary>
/// Background service that:
/// <list type="number">
///   <item>auto-creates the two harbour topics on startup so the user
///         never has to pre-provision anything;</item>
///   <item>publishes a port-call event every 2 s, cycling through a
///         handful of ship/berth combinations so the stream is busy
///         without being noisy;</item>
///   <item>publishes a berth-status snapshot every 5 s on a second
///         topic, giving Bowire two streams to switch between.</item>
/// </list>
/// </summary>
internal sealed class HarborStreamPublisher : BackgroundService
{
    private static readonly (string Name, int Imo)[] Ships =
    [
        ("Nordstern",  9123456),
        ("Isabella",   9234567),
        ("Aurora",     9345678),
        ("Hanseatic",  9456789),
        ("Kraken",     9567890),
    ];

    private static readonly string[] Berths = ["B1", "B2", "B3"];
    private static readonly string[] Phases = ["arriving", "docked", "unloading", "departing"];

    private readonly KafkaSampleOptions _options;
    private readonly ILogger<HarborStreamPublisher> _log;
    // Crypto-strength RNG isn't needed for a sample stream picker.
#pragma warning disable CA5394
    private readonly Random _rng = new();
#pragma warning restore CA5394

    public HarborStreamPublisher(
        IOptions<KafkaSampleOptions> options,
        ILogger<HarborStreamPublisher> log)
    {
        _options = options.Value;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureTopicsAsync(stoppingToken).ConfigureAwait(false);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            Acks = Acks.Leader,
            ClientId = "bowire-kafka-sample",
        };

        using var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();

        // Two independent cadences: 2 s for port calls, 5 s for berth
        // status. Run them on the same loop using a tick counter so we
        // don't need a second BackgroundService.
        var tick = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                ProducePortCall(producer);
                if (tick % 2 == 1) // every other 2 s tick → ~5 s with 2 s base, close enough
                {
                    ProduceBerthStatus(producer);
                }
            }
            catch (ProduceException<string, byte[]> ex)
            {
                _log.LogWarning(ex, "Produce failed: {Reason}", ex.Error.Reason);
            }
            catch (KafkaException ex)
            {
                _log.LogWarning(ex, "Kafka error while publishing");
            }

            tick++;
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        producer.Flush(TimeSpan.FromSeconds(2));
    }

    private void ProducePortCall(IProducer<string, byte[]> producer)
    {
        var ship = Ships[_rng.Next(Ships.Length)];
        var berth = Berths[_rng.Next(Berths.Length)];
        var phase = Phases[_rng.Next(Phases.Length)];
        var teu = _rng.Next(120, 1800);

        var evt = new PortCallEvent(
            ShipName: ship.Name,
            Imo: ship.Imo,
            Berth: berth,
            Kind: phase,
            CargoTeu: teu,
            At: DateTimeOffset.UtcNow);

        var payload = JsonSerializer.SerializeToUtf8Bytes(evt);
        producer.Produce(_options.PortCallsTopic, new Message<string, byte[]>
        {
            // Keying by ship name keeps a single ship's events on the
            // same partition — handy when you scale partitions later.
            Key = ship.Name,
            Value = payload,
        }, DeliveryReport);

        _log.LogInformation("port-call {Ship} {Phase} at {Berth}", ship.Name, phase, berth);
    }

    private void ProduceBerthStatus(IProducer<string, byte[]> producer)
    {
        var berth = Berths[_rng.Next(Berths.Length)];
        var occupied = _rng.Next(2) == 0;
        var ship = occupied ? Ships[_rng.Next(Ships.Length)].Name : null;

        var evt = new BerthStatusEvent(
            Berth: berth,
            State: occupied ? "occupied" : "free",
            CurrentShip: ship,
            QueueLength: _rng.Next(0, 4),
            At: DateTimeOffset.UtcNow);

        var payload = JsonSerializer.SerializeToUtf8Bytes(evt);
        producer.Produce(_options.BerthStatusTopic, new Message<string, byte[]>
        {
            Key = berth,
            Value = payload,
        }, DeliveryReport);
    }

    private void DeliveryReport(DeliveryReport<string, byte[]> dr)
    {
        if (dr.Error.IsError)
        {
            _log.LogWarning(
                "delivery failed: {Topic} → {Reason}",
                dr.Topic, dr.Error.Reason);
        }
    }

    private async Task EnsureTopicsAsync(CancellationToken ct)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = _options.BootstrapServers };
        using var admin = new AdminClientBuilder(adminConfig).Build();

        var wanted = new[]
        {
            new TopicSpecification
            {
                Name = _options.PortCallsTopic,
                NumPartitions = 3,
                ReplicationFactor = 1,
            },
            new TopicSpecification
            {
                Name = _options.BerthStatusTopic,
                NumPartitions = 1,
                ReplicationFactor = 1,
            },
        };

        // Wait for the broker to come up — `docker compose up` returns
        // before Kafka has finished electing itself controller. Up to
        // 30 s of backoff, then give up and let the producer surface
        // the error itself.
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await admin.CreateTopicsAsync(wanted).ConfigureAwait(false);
                _log.LogInformation(
                    "Created topics {Topics}",
                    string.Join(", ", wanted.Select(w => w.Name)));
                return;
            }
            catch (CreateTopicsException ex)
            {
                // TopicAlreadyExists per spec is non-fatal — happens on
                // the second `dotnet run`. Anything else gets logged
                // and we move on; the producer will yell if it's bad.
                if (ex.Results.All(r => !r.Error.IsError ||
                    r.Error.Code == ErrorCode.TopicAlreadyExists))
                {
                    return;
                }

                _log.LogWarning("CreateTopics returned: {Errors}",
                    string.Join("; ", ex.Results.Select(r =>
                        r.Topic + "=" + r.Error.Code.ToString())));
                return;
            }
            catch (KafkaException ex) when (DateTime.UtcNow < deadline)
            {
                _log.LogInformation(
                    "Waiting for Kafka… ({Reason})",
                    ex.Error.Reason);
                await Task.Delay(TimeSpan.FromSeconds(2), ct).ConfigureAwait(false);
            }
        }
    }
}

/// <summary>
/// Small helper for <c>Program.cs</c> readability — keeps the DI wiring
/// out of the entry point.
/// </summary>
internal static class HarborStreamPublisherExtensions
{
    public static IServiceCollection AddHarborStreamPublisher(
        this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaSampleOptions>(configuration.GetSection("Kafka"));
        services.AddHostedService<HarborStreamPublisher>();
        return services;
    }
}
