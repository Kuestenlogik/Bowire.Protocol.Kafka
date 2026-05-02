// Copyright 2026 Küstenlogik
// SPDX-License-Identifier: Apache-2.0

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Confluent.Kafka;
using Kuestenlogik.Bowire.Auth;
using Kuestenlogik.Bowire.Protocol.Kafka;

namespace Kuestenlogik.Bowire.Protocol.Kafka.Tests;

/// <summary>
/// Unit tests for KafkaSecurityConfig — the bridge that pulls Bowire's
/// shared <c>__bowireMtls__</c> + Kafka-specific <c>__bowireKafkaSasl__</c>
/// markers out of the request metadata and onto a librdkafka
/// <see cref="ClientConfig"/>. Verifies the config knobs land on the
/// right fields, that markers get stripped from the returned dict
/// (no secret leaks via Kafka headers), and that the protocol selection
/// (Plaintext / Ssl / SaslPlaintext / SaslSsl) follows the marker mix.
/// </summary>
public class KafkaSecurityConfigTests
{
    [Fact]
    public void Apply_NoMarkers_LeavesConfigUntouched()
    {
        var config = new ProducerConfig();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["key"] = "abc",
            ["partition"] = "0",
        };

        var sanitised = KafkaSecurityConfig.Apply(config, metadata);

        Assert.Null(config.SecurityProtocol);
        Assert.Null(config.SaslMechanism);
        Assert.Null(config.SslCertificatePem);
        // No markers were present, so the dict comes back unchanged.
        Assert.Equal(2, sanitised!.Count);
    }

    [Fact]
    public void Apply_MtlsMarker_SetsSslConfigAndStripsMarker()
    {
        var (certPem, keyPem) = GenerateSelfSignedPem();
        var mtlsJson = $$"""
            {
                "certificate": {{System.Text.Json.JsonSerializer.Serialize(certPem)}},
                "privateKey": {{System.Text.Json.JsonSerializer.Serialize(keyPem)}},
                "allowSelfSigned": true
            }
            """;
        var config = new ProducerConfig();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MtlsConfig.MtlsMarkerKey] = mtlsJson,
            ["key"] = "abc",
        };

        var sanitised = KafkaSecurityConfig.Apply(config, metadata);

        Assert.Equal(SecurityProtocol.Ssl, config.SecurityProtocol);
        Assert.Equal(certPem, config.SslCertificatePem);
        Assert.Equal(keyPem, config.SslKeyPem);
        Assert.False(config.EnableSslCertificateVerification);

        // The mTLS marker must not leak into the Kafka headers — only
        // the genuine "key" entry survives.
        Assert.Single(sanitised!);
        Assert.Equal("abc", sanitised!["key"]);
    }

    [Fact]
    public void Apply_SaslMarkerOnly_SetsSaslPlaintext()
    {
        var saslJson = """{"mechanism":"PLAIN","username":"alice","password":"s3cret"}""";
        var config = new ConsumerConfig { GroupId = "test" };
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [KafkaSecurityConfig.SaslMarkerKey] = saslJson,
        };

        var sanitised = KafkaSecurityConfig.Apply(config, metadata);

        Assert.Equal(SecurityProtocol.SaslPlaintext, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.Plain, config.SaslMechanism);
        Assert.Equal("alice", config.SaslUsername);
        Assert.Equal("s3cret", config.SaslPassword);
        Assert.Empty(sanitised!);
    }

    [Fact]
    public void Apply_BothMarkers_PromotesProtocolToSaslSsl()
    {
        var (certPem, keyPem) = GenerateSelfSignedPem();
        var mtlsJson = $$"""
            {
                "certificate": {{System.Text.Json.JsonSerializer.Serialize(certPem)}},
                "privateKey": {{System.Text.Json.JsonSerializer.Serialize(keyPem)}}
            }
            """;
        var saslJson = """{"mechanism":"SCRAM-SHA-512","username":"bob","password":"hunter2"}""";
        var config = new AdminClientConfig();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MtlsConfig.MtlsMarkerKey] = mtlsJson,
            [KafkaSecurityConfig.SaslMarkerKey] = saslJson,
        };

        KafkaSecurityConfig.Apply(config, metadata);

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal("bob", config.SaslUsername);
        Assert.Equal(certPem, config.SslCertificatePem);
    }

    [Fact]
    public void Apply_MalformedSaslJson_FallsThroughCleanly()
    {
        // A garbage marker shouldn't crash the producer build — the
        // helper just leaves the SASL fields unset and the config can
        // still talk to a plaintext broker.
        var config = new ProducerConfig();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [KafkaSecurityConfig.SaslMarkerKey] = "not valid json",
            ["key"] = "abc",
        };

        var sanitised = KafkaSecurityConfig.Apply(config, metadata);

        Assert.Null(config.SaslMechanism);
        Assert.Null(config.SecurityProtocol);
        // Malformed marker still gets stripped — leaking a bogus blob
        // as a Kafka header would only confuse the broker.
        Assert.Single(sanitised!);
        Assert.False(sanitised!.ContainsKey(KafkaSecurityConfig.SaslMarkerKey));
    }

    [Fact]
    public void SaslConfig_TryParse_RejectsMissingFields()
    {
        Assert.Null(KafkaSecurityConfig.SaslConfig.TryParse("""{"mechanism":"PLAIN"}"""));
        Assert.Null(KafkaSecurityConfig.SaslConfig.TryParse("""{"username":"alice"}"""));
        Assert.Null(KafkaSecurityConfig.SaslConfig.TryParse("not json"));
    }

    [Fact]
    public void SaslConfig_TryParse_DefaultsMechanismToPlain()
    {
        var cfg = KafkaSecurityConfig.SaslConfig.TryParse("""{"username":"a","password":"b"}""");
        Assert.NotNull(cfg);
        Assert.Equal("PLAIN", cfg!.Mechanism);
    }

    private static (string CertPem, string KeyPem) GenerateSelfSignedPem()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=kafka-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow.AddYears(1));

        var certPem = "-----BEGIN CERTIFICATE-----\n"
            + Convert.ToBase64String(cert.Export(X509ContentType.Cert), Base64FormattingOptions.InsertLineBreaks)
            + "\n-----END CERTIFICATE-----";
        var keyPem = "-----BEGIN PRIVATE KEY-----\n"
            + Convert.ToBase64String(rsa.ExportPkcs8PrivateKey(), Base64FormattingOptions.InsertLineBreaks)
            + "\n-----END PRIVATE KEY-----";
        return (certPem, keyPem);
    }
}
