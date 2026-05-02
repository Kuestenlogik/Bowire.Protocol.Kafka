// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Confluent.Kafka;
using Kuestenlogik.Bowire.Auth;

namespace Kuestenlogik.Bowire.Protocol.Kafka;

/// <summary>
/// Pulls Bowire auth markers out of the request metadata and maps them
/// onto the librdkafka <c>ClientConfig</c> security knobs. Two markers
/// flow in:
/// <list type="bullet">
/// <item>
///   <c>__bowireMtls__</c> — shared with REST/gRPC/WebSocket/SignalR.
///   PEM cert + key + optional CA / passphrase / allow-self-signed.
///   Maps to <c>SecurityProtocol = Ssl</c> +
///   <c>SslCertificatePem</c> / <c>SslKeyPem</c> /
///   <c>SslCaPem</c> / <c>EnableSslCertificateVerification</c>.
/// </item>
/// <item>
///   <c>__bowireKafkaSasl__</c> — Kafka-specific JSON
///   <c>{ mechanism, username, password }</c>. Mechanism is one of
///   <c>PLAIN</c>, <c>SCRAM-SHA-256</c>, <c>SCRAM-SHA-512</c>,
///   <c>OAUTHBEARER</c>. Combines with the mTLS marker to set
///   <c>SecurityProtocol = SaslSsl</c>; alone it sets
///   <c>SecurityProtocol = SaslPlaintext</c>.
/// </item>
/// </list>
/// </summary>
internal static class KafkaSecurityConfig
{
    /// <summary>Magic metadata key for SASL credentials.</summary>
    public const string SaslMarkerKey = "__bowireKafkaSasl__";

    /// <summary>
    /// Inspect the metadata for security markers and apply the matching
    /// librdkafka knobs to <paramref name="config"/>. Returns the same
    /// dictionary with the markers stripped so plugin callers can pass
    /// the rest along as Kafka headers without leaking secrets.
    /// </summary>
    public static Dictionary<string, string>? Apply(
        ClientConfig config, Dictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0) return metadata;

        var mtls = MtlsConfig.TryParseFromMetadata(metadata);
        SaslConfig? sasl = null;
        if (metadata.TryGetValue(SaslMarkerKey, out var saslJson))
        {
            sasl = SaslConfig.TryParse(saslJson);
        }

        if (mtls is not null) ApplyMtls(config, mtls);
        if (sasl is not null) ApplySasl(config, sasl);

        // Combined SASL + SSL → SaslSsl. SASL alone → SaslPlaintext.
        // mTLS alone → Ssl (already set by ApplyMtls).
        if (mtls is not null && sasl is not null)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
        }
        else if (sasl is not null)
        {
            config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
        }

        // Strip both markers so the rest of the metadata can pass
        // through as Kafka headers without leaking secrets on the wire.
        var sanitised = new Dictionary<string, string>(metadata.Count, StringComparer.Ordinal);
        foreach (var (k, v) in metadata)
        {
            if (string.Equals(k, MtlsConfig.MtlsMarkerKey, StringComparison.Ordinal)) continue;
            if (string.Equals(k, SaslMarkerKey, StringComparison.Ordinal)) continue;
            sanitised[k] = v;
        }
        return sanitised;
    }

    private static void ApplyMtls(ClientConfig config, MtlsConfig mtls)
    {
        config.SecurityProtocol = SecurityProtocol.Ssl;
        // librdkafka 2.x accepts PEM strings directly via the
        // *Pem properties — no temp files needed, the certs travel
        // through memory only.
        config.SslCertificatePem = mtls.CertificatePem;
        config.SslKeyPem = mtls.PrivateKeyPem;
        if (!string.IsNullOrEmpty(mtls.Passphrase))
        {
            config.SslKeyPassword = mtls.Passphrase;
        }
        if (!string.IsNullOrEmpty(mtls.CaCertificatePem))
        {
            config.SslCaPem = mtls.CaCertificatePem;
        }
        if (mtls.AllowSelfSigned)
        {
            // librdkafka exposes "skip cert verification" as a single
            // boolean — coarser than the REST CA-pinning validator,
            // but matches the "Allow self-signed" UI semantics.
            config.EnableSslCertificateVerification = false;
        }
    }

    private static void ApplySasl(ClientConfig config, SaslConfig sasl)
    {
        config.SaslMechanism = sasl.Mechanism switch
        {
            "PLAIN" => SaslMechanism.Plain,
            "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
            "SCRAM-SHA-512" => SaslMechanism.ScramSha512,
            "OAUTHBEARER" => SaslMechanism.OAuthBearer,
            _ => SaslMechanism.Plain,
        };
        config.SaslUsername = sasl.Username;
        config.SaslPassword = sasl.Password;
    }

    /// <summary>
    /// Parsed SASL credentials carried inline in the metadata dict via
    /// <see cref="SaslMarkerKey"/>.
    /// </summary>
    internal sealed record SaslConfig(string Mechanism, string Username, string Password)
    {
        public static SaslConfig? TryParse(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;
                if (root.ValueKind != JsonValueKind.Object) return null;

                string? Get(string name) =>
                    root.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String
                        ? p.GetString()
                        : null;

                var mechanism = (Get("mechanism") ?? "PLAIN").ToUpperInvariant();
                var username = Get("username");
                var password = Get("password");
                if (string.IsNullOrEmpty(username) || password is null) return null;

                return new SaslConfig(mechanism, username, password);
            }
            catch (JsonException)
            {
                return null;
            }
        }
    }
}
