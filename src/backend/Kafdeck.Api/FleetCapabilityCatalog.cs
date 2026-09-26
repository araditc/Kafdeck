namespace Kafdeck.Api;

public enum FleetCapabilityState
{
    Supported = 1,
    Unsupported = 2,
    Blocked = 3,
    Unconfigured = 4,
    Unknown = 5,
}

public sealed record FleetCapabilityStatus(
    string Id,
    string DisplayName,
    FleetCapabilityState State,
    string Reason,
    string Workstream,
    IReadOnlyList<string> Evidence);

/// <summary>
/// v0.6 release-time capability truth. This catalog intentionally distinguishes
/// implemented provider/runtime support from explicit pinned-client gaps and
/// governance-blocked activation paths. It is not inferred from empty provider
/// responses and it never upgrades a capability based on Kafka version alone.
/// </summary>
public static class FleetCapabilityCatalog
{
    private static readonly IReadOnlyList<FleetCapabilityStatus> Items =
        Array.AsReadOnly(new[]
        {
            Blocked(
                "acl-administration",
                "ACL administration",
                "W42",
                "Typed ACL provider/domain contracts are admitted, but the v0.6 production API/runtime does not register the W42 mutation handler surface; activation therefore remains fail-closed.",
                "Confluent.Kafka 2.15.1: DescribeAclsAsync/CreateAclsAsync/DeleteAclsAsync",
                "production Program.cs has no W42 handler registration"),
            Blocked(
                "scram-administration",
                "SCRAM administration",
                "W43",
                "Typed SCRAM provider/domain contracts are admitted, but the v0.6 production API/runtime does not register the W43 mutation handler surface; secret-changing activation remains fail-closed.",
                "Confluent.Kafka 2.15.1: DescribeUserScramCredentialsAsync/AlterUserScramCredentialsAsync",
                "production Program.cs has no W43 handler registration"),
            Blocked(
                "dynamic-configuration",
                "Allowlisted dynamic configuration",
                "W44",
                "Typed dynamic-config provider/domain contracts are admitted, but the v0.6 production API/runtime does not register the W44 mutation handler surface; public mutation activation remains fail-closed.",
                "Confluent.Kafka 2.15.1: DescribeConfigsAsync/IncrementalAlterConfigsAsync",
                "production Program.cs has no W44 handler registration"),
            Unsupported(
                "client-quotas",
                "Client quotas",
                "W44",
                "Pinned Confluent.Kafka 2.15.1 exposes no typed ClientQuotas describe/alter API; no escape tunnel is permitted.",
                "compile-probe: DescribeClientQuotasAsync absent",
                "compile-probe: AlterClientQuotasAsync absent"),
            Blocked(
                "preferred-leader-election",
                "Preferred leader election",
                "W45",
                "Pinned provider primitive exists, but the v0.6 public execution path is not activated without the full W45 safety/readback contract.",
                "Confluent.Kafka 2.15.1: ElectLeadersAsync present"),
            Unsupported(
                "partition-reassignment",
                "Partition reassignment",
                "W45",
                "Pinned Confluent.Kafka 2.15.1 exposes no typed reassignment submit/list API; no CLI/raw-protocol/sidecar bypass is permitted.",
                "compile-probe: AlterPartitionReassignmentsAsync absent",
                "compile-probe: ListPartitionReassignmentsAsync absent"),
            Unsupported(
                "replication-factor-change",
                "Replication-factor change",
                "W45",
                "The admitted RF path depends on typed partition reassignment, which is unavailable in the pinned provider client.",
                "dependency: partition-reassignment unsupported"),
            Blocked(
                "reassignment-throttles",
                "Reassignment throttles",
                "W45",
                "Throttle mutation is not exposed independently when the corresponding reassignment operation cannot be safely admitted.",
                "ordinary W44 dynamic-config path rejects throttle keys"),
            Blocked(
                "broker-maintenance",
                "Broker maintenance",
                "W46",
                "Cordon/evacuation/decommission activation remains blocked because required typed reassignment/unregister primitives are not all available.",
                "dependency: partition-reassignment unsupported",
                "no shell/controller bypass"),
            Unsupported(
                "log-directory-maintenance",
                "Log-directory maintenance",
                "W46",
                "No complete typed pinned-client path is admitted for the required directory movement/removal lifecycle.",
                "no filesystem/CLI/controller bypass"),
            Blocked(
                "finite-cluster-transfer",
                "Finite cluster transfer",
                "W47",
                "Planning, physical-cluster binding, durable dispatch/checkpoints and bounded source-range reading are implemented; long-running public activation remains blocked until its full current-identity orchestration path is admitted.",
                "W47 PRs #165/#166/#171",
                "no replay after unresolved DispatchStarted"),
            Blocked(
                "managed-replication",
                "Managed MirrorMaker 2 activation",
                "W48",
                "The admitted v0.6 baseline explicitly blocks create/update/resume/restart/task-restart activation because standard MM2 cannot enforce Kafdeck lifetime/rate/revocation budgets.",
                "shared ConnectReplicationActivationGuard",
                "safe observation/pause boundary only"),
        });

    public static IReadOnlyList<FleetCapabilityStatus> All => Items;

    public static FleetCapabilityStatus Require(string id) =>
        Items.FirstOrDefault(item => string.Equals(item.Id, id, StringComparison.Ordinal))
        ?? throw new KeyNotFoundException($"Unknown fleet capability '{id}'.");

    private static FleetCapabilityStatus Supported(
        string id,
        string name,
        string workstream,
        string reason,
        params string[] evidence) =>
        Create(id, name, FleetCapabilityState.Supported, reason, workstream, evidence);

    private static FleetCapabilityStatus Unsupported(
        string id,
        string name,
        string workstream,
        string reason,
        params string[] evidence) =>
        Create(id, name, FleetCapabilityState.Unsupported, reason, workstream, evidence);

    private static FleetCapabilityStatus Blocked(
        string id,
        string name,
        string workstream,
        string reason,
        params string[] evidence) =>
        Create(id, name, FleetCapabilityState.Blocked, reason, workstream, evidence);

    private static FleetCapabilityStatus Create(
        string id,
        string name,
        FleetCapabilityState state,
        string reason,
        string workstream,
        IReadOnlyList<string> evidence)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        ArgumentException.ThrowIfNullOrWhiteSpace(workstream);

        if (id.Length > 96 ||
            id.Any(character => !(char.IsAsciiLower(character) || char.IsAsciiDigit(character) || character == '-')) ||
            reason.Length > 1024 ||
            workstream.Length > 16 ||
            evidence.Count > 8 ||
            evidence.Any(item => string.IsNullOrWhiteSpace(item) || item.Length > 512 || item.Any(char.IsControl)))
        {
            throw new InvalidOperationException("Fleet capability catalog contains an invalid bounded entry.");
        }

        return new FleetCapabilityStatus(
            id,
            name,
            state,
            reason,
            workstream,
            Array.AsReadOnly(evidence.ToArray()));
    }
}
