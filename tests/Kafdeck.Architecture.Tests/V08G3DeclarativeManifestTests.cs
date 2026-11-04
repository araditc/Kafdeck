using System.Text;
using Kafdeck.Core.Declarative;
using Kafdeck.Core.Notifications;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G3DeclarativeManifestTests
{
    internal const string Json = """
        {"apiVersion":"kafdeck.io/v0.8","kind":"NotificationSubscription","metadata":{"deploymentRef":"production-a","name":"ops-lag","ownerRef":"team-ops"},"spec":{"destinationRef":"ops-notifier","state":"Active","eventClasses":["Mutation","Operational"],"eventTypes":["mutation.state-changed","consumer.lag.alert"]}}
        """;
    internal const string Yaml = """
        apiVersion: kafdeck.io/v0.8
        kind: NotificationSubscription
        metadata:
          ownerRef: team-ops
          name: ops-lag
          deploymentRef: production-a
        spec:
          eventTypes:
            - consumer.lag.alert
            - 'mutation.state-changed'
          state: "Active"
          destinationRef: ops-notifier
          eventClasses: [Operational, Mutation]
        """;
    private static readonly DateTimeOffset Now = new(2026, 11, 4, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Json_and_safe_yaml_share_exact_canonical_bytes_hash_and_underlying_set_normalization()
    {
        var json = Parse(Json); var yaml = Parse(Yaml, "yaml");
        Assert.Equal(json.ToCanonicalUtf8(), yaml.ToCanonicalUtf8());
        Assert.Equal(json.Fingerprint(), yaml.Fingerprint());
        Assert.Equal(json.Fingerprint(), DeclarativeSubscriptionManifest.Parse(json.ToCanonicalUtf8(), "json").Fingerprint());
        Assert.Equal(64, json.Fingerprint().Length);
        Assert.NotEqual(json.Fingerprint(), Parse(Json.Replace("team-ops", "team-b", StringComparison.Ordinal)).Fingerprint());
        Assert.NotEqual(json.Fingerprint(), Parse(Json.Replace("production-a", "production-b", StringComparison.Ordinal)).Fingerprint());
        Assert.Equal("ops-lag", json.Definition.SubscriptionId);
    }

    [Theory]
    [InlineData("metadata: &anchor")]
    [InlineData("spec: *alias")]
    [InlineData("spec: !custom")]
    [InlineData("spec: !!map")]
    [InlineData("spec: {state: Active}")]
    [InlineData("spec:\n  <<: *merge")]
    [InlineData("apiVersion: kafdeck.io/v0.8\nkind: NotificationSubscription\nmetadata: &recursive [*recursive]")]
    public void Yaml_graphs_tags_and_flow_maps_are_unconditionally_rejected_before_construction(string yaml) =>
        Assert.Throws<ArgumentException>(() => Parse(yaml, "yaml"));

    [Theory]
    [InlineData("apiVersion: kafdeck.io/v0.8\napiVersion: kafdeck.io/v0.8")]
    [InlineData("spec:\n  password: sentinel-credential")]
    [InlineData("spec:\n  eventTypes: |\n    arbitrary-code")]
    [InlineData("spec:\n    state: Active")]
    [InlineData("%YAML 1.2\n---")]
    public void Yaml_duplicate_unknown_secret_and_nonadmitted_shapes_fail_closed_without_echo(string yaml)
    {
        var error = Assert.Throws<ArgumentException>(() => Parse(yaml, "yaml"));
        Assert.DoesNotContain("sentinel", error.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\"kind\":\"NotificationSubscription\"", "\"kind\":\"KafkaTopic\"")]
    [InlineData("kafdeck.io/v0.8", "kafdeck.io/v0.8-proposal")]
    [InlineData("\"state\":\"Active\"", "\"state\":\"Retired\"")]
    [InlineData("\"ownerRef\":\"team-ops\"", "\"ownerRef\":\"team-ops\",\"password\":\"sentinel-credential\"")]
    [InlineData("\"name\":\"ops-lag\"", "\"name\":\"ops-lag\",\"name\":\"other\"")]
    [InlineData("\"name\":\"ops-lag\"", "\"name\":\".\"")]
    [InlineData("\"name\":\"ops-lag\"", "\"name\":\"..\"")]
    [InlineData("\"Operational\"", "\"Mutation\"")]
    public void Json_unknowns_duplicates_versions_retirement_and_duplicate_filters_are_denied(string before, string after)
    {
        var error = Assert.Throws<ArgumentException>(() => Parse(Json.Replace(before, after, StringComparison.Ordinal)));
        Assert.DoesNotContain("sentinel", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Independent_byte_depth_node_unicode_and_string_budgets_fail_closed()
    {
        Assert.Throws<ArgumentException>(() => DeclarativeSubscriptionManifest.Parse(new byte[DeclarativeSubscriptionManifest.MaxInputBytes + 1], "json"));
        Assert.Throws<ArgumentException>(() => DeclarativeSubscriptionManifest.Parse(new byte[] { 0xc0, 0xaf }, "yaml"));
        Assert.Throws<ArgumentException>(() => Parse("{\"x\":" + new string('[', 10) + "0" + new string(']', 10) + "}"));
        Assert.Throws<ArgumentException>(() => Parse("[" + string.Join(',', Enumerable.Repeat("\"a\"", 257)) + "]"));
        Assert.Throws<ArgumentException>(() => Parse(Json.Replace("team-ops", new string('a', 1025), StringComparison.Ordinal)));
        Assert.Throws<ArgumentException>(() => Parse(Json.Replace("team-ops", "\\ud800", StringComparison.Ordinal)));
        Assert.Throws<ArgumentException>(() => Parse(Yaml + "\n" + new string('\n', 257), "yaml"));
        Assert.Throws<ArgumentException>(() => Parse(Json, "xml"));
    }

    [Theory]
    [InlineData(DeclarativeDriftState.Inaccessible)]
    [InlineData(DeclarativeDriftState.Unsupported)]
    public void Authorization_and_capability_truth_dominate_hidden_identity_or_absence(DeclarativeDriftState truth)
    {
        var observed = new DeclarativeSubscriptionObservation("hidden", "hidden", truth, default);
        Assert.Equal(truth, Compare(observed));
    }

    [Fact]
    public void Drift_has_all_seven_truths_and_never_creates_from_uncertain_observation()
    {
        var desired = Parse(Json);
        var snapshot = new NotificationSubscriptionSnapshot(desired.Definition, desired.State, 1, Now);
        var current = new DeclarativeSubscriptionObservation(desired.DeploymentRef, desired.Definition.SubscriptionId,
            DeclarativeDriftState.InSync, Now, snapshot, desired.OwnerRef);
        Assert.Equal(DeclarativeDriftState.InSync, Compare(current));
        Assert.Equal(DeclarativeDriftState.OutOfSync, Compare(current with { Snapshot = new(desired.Definition, NotificationSubscriptionState.Paused, 2, Now) }));
        Assert.Equal(DeclarativeDriftState.Missing, Compare(current with { ReadState = DeclarativeDriftState.Missing, Snapshot = null }));
        Assert.Equal(DeclarativeDriftState.Unknown, Compare(current with { ReadState = DeclarativeDriftState.Missing }));
        Assert.Equal(DeclarativeDriftState.Unknown, Compare(current with { ObservedAtUtc = Now.AddMinutes(-2) }));
        Assert.Equal(DeclarativeDriftState.Unknown, Compare(current with { ObservedAtUtc = Now.AddSeconds(1) }));
        Assert.Equal(DeclarativeDriftState.Conflict, Compare(current with { ManagedOwnerRef = "foreign-owner" }));
        Assert.Equal(DeclarativeDriftState.Conflict, Compare(current with { Snapshot = new(desired.Definition, NotificationSubscriptionState.Retired, 2, Now) }));
        Assert.Equal(DeclarativeDriftState.Unknown, Compare(current with { ReadState = (DeclarativeDriftState)999 }));
    }
    private static DeclarativeSubscriptionManifest Parse(string text, string format = "json") =>
        DeclarativeSubscriptionManifest.Parse(Encoding.UTF8.GetBytes(text), format);
    private static DeclarativeDriftState Compare(DeclarativeSubscriptionObservation observation) =>
        DeclarativeSubscriptionDrift.Compare(Parse(Json), observation, Now, TimeSpan.FromMinutes(1));
}
