using System.Text;
using Kafdeck.Core.Declarative;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08W68CandidateManifestContractTests
{
    private const string Valid = """
        {
          "apiVersion":"kafdeck.io/v0.8-proposal",
          "kind":"NotificationSubscription",
          "metadata":{"deploymentRef":"production-a","name":"ops-lag","ownerRef":"team-ops"},
          "spec":{"destinationRef":"ops-notifier","state":"Active","eventClasses":["Operational"],"eventTypes":["consumer.lag.alert"]}
        }
        """;

    [Fact]
    public void Canonical_fingerprint_is_stable_for_reordered_json_keys_and_untrusted_owner_labels()
    {
        var first = CandidateSubscriptionManifest.ParseJson(Encoding.UTF8.GetBytes(Valid));
        const string reordered = """
            {"kind":"NotificationSubscription","spec":{"eventTypes":["consumer.lag.alert"],"eventClasses":["Operational"],"state":"Active","destinationRef":"ops-notifier"},"metadata":{"ownerRef":"team-ops","name":"ops-lag","deploymentRef":"production-a"},"apiVersion":"kafdeck.io/v0.8-proposal"}
            """;
        var second = CandidateSubscriptionManifest.ParseJson(Encoding.UTF8.GetBytes(reordered));
        Assert.Equal("production-a", first.DeploymentRef);
        Assert.Equal("team-ops", first.OwnerRef);
        Assert.Equal(first.Fingerprint(), second.Fingerprint());
        Assert.Equal(64, first.Fingerprint().Length);
    }

    [Theory]
    [InlineData("""{"apiVersion":"kafdeck.io/v0.8-proposal","kind":"NotificationSubscription","metadata":{"name":"ops-lag","ownerRef":"team-ops"},"spec":{"destinationRef":"ops-notifier","state":"Active","eventClasses":["Operational"],"eventTypes":["consumer.lag.alert"]}}""")]
    [InlineData("""{"apiVersion":"kafdeck.io/v0.8-proposal","kind":"NotificationSubscription","metadata":{"deploymentRef":"production-a","name":"ops-lag","ownerRef":"team-ops"},"spec":{"destinationRef":"ops-notifier","state":"Active","eventClasses":["Operational"],"eventTypes":["consumer.lag.alert"],"bearer":"secret"}}""")]
    [InlineData("""{"apiVersion":"kafdeck.io/v0.8-proposal","kind":"NotificationSubscription","metadata":{"deploymentRef":"production-a","deploymentRef":"production-b","name":"ops-lag","ownerRef":"team-ops"},"spec":{"destinationRef":"ops-notifier","state":"Active","eventClasses":["Operational"],"eventTypes":["consumer.lag.alert"]}}""")]
    [InlineData("""{"apiVersion":"kafdeck.io/v0.8-proposal","kind":"GenericHttp","metadata":{"deploymentRef":"production-a","name":"ops-lag","ownerRef":"team-ops"},"spec":{"destinationRef":"ops-notifier","state":"Active","eventClasses":["Operational"],"eventTypes":["consumer.lag.alert"]}}""")]
    public void Parser_rejects_missing_deployment_unknown_secret_duplicate_field_and_generic_kind(string payload)
    {
        Assert.Throws<ArgumentException>(() => CandidateSubscriptionManifest.ParseJson(Encoding.UTF8.GetBytes(payload)));
    }

    [Fact]
    public void Parser_fails_closed_on_excessive_bytes_and_yaml_aliases()
    {
        var oversized = new byte[CandidateSubscriptionManifest.MaxInputBytes + 1];
        Array.Fill(oversized, (byte)'x');
        Assert.Throws<ArgumentException>(() => CandidateSubscriptionManifest.ParseJson(oversized));
        const string yaml = """
            apiVersion: kafdeck.io/v0.8-proposal
            kind: NotificationSubscription
            metadata: &scope
              deploymentRef: production-a
            spec:
              metadata: *scope
            """;
        Assert.Throws<ArgumentException>(() => CandidateSubscriptionManifest.ParseJson(Encoding.UTF8.GetBytes(yaml)));
    }

    [Fact]
    public void Observed_candidate_manifest_is_not_an_ownership_or_apply_authority()
    {
        var manifest = CandidateSubscriptionManifest.ParseJson(Encoding.UTF8.GetBytes(Valid));
        Assert.Equal("ops-lag", manifest.Definition.SubscriptionId);
        Assert.Equal("ops-notifier", manifest.Definition.DestinationId);
        Assert.NotEqual(string.Empty, manifest.Fingerprint());
        // No claimed owner registry, plan ID or execution is produced by parsing.
        Assert.DoesNotContain(typeof(CandidateSubscriptionManifest).GetMethods(),
            method => method.Name is "Apply" or "Adopt" or "Execute" or "Destroy");
    }
}
