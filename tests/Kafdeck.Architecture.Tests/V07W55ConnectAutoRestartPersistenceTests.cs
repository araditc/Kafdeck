using System.Security.Cryptography;
using System.Text;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Connect;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V07W55ConnectAutoRestartPersistenceTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 27, 7, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Sqlite_state_is_durable_cas_guarded_and_does_not_reset_on_reload()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w55-{Guid.NewGuid():N}.db");

        try
        {
            var factory =
                new SqliteMutationDbConnectionFactory(path);
            var store =
                new AdoConnectAutoRestartStateStore(factory);
            await store.InitializeAsync();

            var activation = Activation(
                "sink-a",
                profile: "default");

            Assert.True(
                await store.TryCreateAsync(
                    activation,
                    aggregateProfileLimit: 10));

            var lease = await store.TryAcquireLeaseAsync(
                activation.ActivationId,
                "worker-a",
                Now,
                TimeSpan.FromSeconds(30));
            Assert.NotNull(lease);

            var dispatchId = Guid.NewGuid();
            var reserved =
                activation.ReserveAttempt(
                    Now,
                    dispatchId);

            Assert.True(
                await store.TryUpdateAsync(
                    reserved,
                    expectedVersion: activation.Version,
                    lease!,
                    Now));

            var reloaded =
                new AdoConnectAutoRestartStateStore(
                    new SqliteMutationDbConnectionFactory(path));
            await reloaded.InitializeAsync();

            var persisted =
                await reloaded.GetAsync(
                    activation.ActivationId);

            Assert.NotNull(persisted);
            Assert.Equal(1, persisted!.AttemptsUsed);
            Assert.True(persisted.HasUnresolvedDispatch);
            Assert.Equal(dispatchId, persisted.UnresolvedDispatchId);
            Assert.Equal(reserved.DeadlineUtc, persisted.DeadlineUtc);
            Assert.Equal(reserved.NextAttemptUtc, persisted.NextAttemptUtc);

            Assert.False(
                await reloaded.TryUpdateAsync(
                    reserved,
                    expectedVersion: activation.Version,
                    lease!,
                    Now));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Profile_slot_cap_is_atomic_and_terminal_release_allows_new_activation()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w55-cap-{Guid.NewGuid():N}.db");

        try
        {
            var store =
                new AdoConnectAutoRestartStateStore(
                    new SqliteMutationDbConnectionFactory(path));
            await store.InitializeAsync();

            var first = Activation("sink-a", "analytics");
            var second = Activation("sink-b", "analytics");

            Assert.True(
                await store.TryCreateAsync(
                    first,
                    aggregateProfileLimit: 1));
            Assert.False(
                await store.TryCreateAsync(
                    second,
                    aggregateProfileLimit: 1));

            var lease = await store.TryAcquireLeaseAsync(
                first.ActivationId,
                "worker-a",
                Now,
                TimeSpan.FromSeconds(30));
            Assert.NotNull(lease);

            var dispatchId = Guid.NewGuid();
            var reserved = first.ReserveAttempt(
                Now,
                dispatchId);
            var recovered =
                reserved.RecordRecovered(
                    dispatchId);

            Assert.True(
                await store.TryUpdateAsync(
                    reserved,
                    first.Version,
                    lease!,
                    Now));
            Assert.True(
                await store.TryUpdateAsync(
                    recovered,
                    reserved.Version,
                    lease!,
                    Now));

            Assert.True(
                await store.TryCreateAsync(
                    second,
                    aggregateProfileLimit: 1));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Ambiguous_activation_retains_profile_slot_and_blocks_reuse()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w55-ambiguous-{Guid.NewGuid():N}.db");

        try
        {
            var store =
                new AdoConnectAutoRestartStateStore(
                    new SqliteMutationDbConnectionFactory(path));
            await store.InitializeAsync();

            var first = Activation("sink-a", "analytics");
            Assert.True(
                await store.TryCreateAsync(
                    first,
                    aggregateProfileLimit: 1));

            var lease = await store.TryAcquireLeaseAsync(
                first.ActivationId,
                "worker-a",
                Now,
                TimeSpan.FromSeconds(30));
            Assert.NotNull(lease);

            var dispatchId = Guid.NewGuid();
            var reserved =
                first.ReserveAttempt(
                    Now,
                    dispatchId);
            Assert.True(
                await store.TryUpdateAsync(
                    reserved,
                    first.Version,
                    lease!,
                    Now));

            var ambiguous =
                reserved.RecordAmbiguous(
                    dispatchId,
                    "provider_outcome_unknown");
            Assert.True(
                await store.TryUpdateAsync(
                    ambiguous,
                    reserved.Version,
                    lease!,
                    Now));

            var competitor =
                Activation("sink-b", "analytics");
            Assert.False(
                await store.TryCreateAsync(
                    competitor,
                    aggregateProfileLimit: 1));

            var active =
                await store.ListActiveAsync(
                    "prod",
                    "analytics",
                    10);
            var persisted = Assert.Single(active);
            Assert.Equal(
                ConnectAutoRestartCircuitState.Ambiguous,
                persisted.CircuitState);
            Assert.True(persisted.HasUnresolvedDispatch);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Duplicate_target_claim_is_rejected_even_when_profile_has_free_slots()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w55-target-{Guid.NewGuid():N}.db");

        try
        {
            var store =
                new AdoConnectAutoRestartStateStore(
                    new SqliteMutationDbConnectionFactory(path));
            await store.InitializeAsync();

            var first = Activation("sink-a", "default");
            var duplicate =
                Activation("sink-a", "default");

            Assert.True(
                await store.TryCreateAsync(
                    first,
                    aggregateProfileLimit: 10));
            Assert.False(
                await store.TryCreateAsync(
                    duplicate,
                    aggregateProfileLimit: 10));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Lease_fencing_prevents_two_workers_and_rejects_stale_generation()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w55-lease-{Guid.NewGuid():N}.db");

        try
        {
            var store =
                new AdoConnectAutoRestartStateStore(
                    new SqliteMutationDbConnectionFactory(path));
            await store.InitializeAsync();

            var activation = Activation(
                "sink-a",
                "analytics");
            Assert.True(
                await store.TryCreateAsync(
                    activation,
                    aggregateProfileLimit: 2));

            var firstLease = await store.TryAcquireLeaseAsync(
                activation.ActivationId,
                "worker-a",
                Now,
                TimeSpan.FromSeconds(10));
            Assert.NotNull(firstLease);

            var competingLease = await store.TryAcquireLeaseAsync(
                activation.ActivationId,
                "worker-b",
                Now.AddSeconds(1),
                TimeSpan.FromSeconds(10));
            Assert.Null(competingLease);

            var takeoverLease = await store.TryAcquireLeaseAsync(
                activation.ActivationId,
                "worker-b",
                Now.AddSeconds(11),
                TimeSpan.FromSeconds(10));
            Assert.NotNull(takeoverLease);
            Assert.True(
                takeoverLease!.Generation >
                firstLease!.Generation);

            var dispatchId = Guid.NewGuid();
            var reserved =
                activation.ReserveAttempt(
                    Now.AddSeconds(11),
                    dispatchId);

            Assert.False(
                await store.TryUpdateAsync(
                    reserved,
                    activation.Version,
                    firstLease,
                    Now.AddSeconds(11)));

            Assert.True(
                await store.TryUpdateAsync(
                    reserved,
                    activation.Version,
                    takeoverLease,
                    Now.AddSeconds(11)));
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    [Fact]
    public async Task Disabled_policy_persists_without_consuming_active_profile_slot()
    {
        var path = Path.Combine(
            Path.GetTempPath(),
            $"kafdeck-w55-disabled-{Guid.NewGuid():N}.db");

        try
        {
            var store =
                new AdoConnectAutoRestartStateStore(
                    new SqliteMutationDbConnectionFactory(path));
            await store.InitializeAsync();

            var disabled =
                Activation(
                    "sink-disabled",
                    "default",
                    ConnectAutoRestartPolicy.Disabled);
            Assert.True(
                await store.TryCreateAsync(
                    disabled,
                    aggregateProfileLimit: 1));

            var enabled =
                Activation(
                    "sink-enabled",
                    "default");
            Assert.True(
                await store.TryCreateAsync(
                    enabled,
                    aggregateProfileLimit: 1));

            var active =
                await store.ListActiveAsync(
                    "prod",
                    "default",
                    10);

            Assert.Single(active);
            Assert.Equal(
                enabled.ActivationId,
                active[0].ActivationId);
        }
        finally
        {
            DeleteSqliteFiles(path);
        }
    }

    private static ConnectAutoRestartActivation Activation(
        string connector,
        string profile,
        ConnectAutoRestartPolicy? policy = null) =>
        ConnectAutoRestartActivation.Create(
            new ConnectAutoRestartTarget(
                "prod",
                profile,
                connector,
                null),
            policy ?? new ConnectAutoRestartPolicy(
                enabled: true,
                jitterBasisPoints: 0),
            Now,
            Fingerprint($"provider:{profile}"),
            Fingerprint($"config:{connector}"),
            Fingerprint("policy"),
            "oidc:https://idp.example|kafdeck-auto-restart",
            Guid.NewGuid());

    private static string Fingerprint(string value) =>
        Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(value)))
            .ToLowerInvariant();

    private static void DeleteSqliteFiles(
        string databasePath)
    {
        foreach (var file in new[]
                 {
                     databasePath,
                     databasePath + "-wal",
                     databasePath + "-shm",
                 })
        {
            try
            {
                File.Delete(file);
            }
            catch (IOException)
            {
                // Best-effort cleanup after all scoped connections close.
            }
        }
    }
}
