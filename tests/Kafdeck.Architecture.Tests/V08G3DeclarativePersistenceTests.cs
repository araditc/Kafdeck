using Kafdeck.Core.Notifications;
using Kafdeck.Infrastructure.Persistence;
using Xunit;

namespace Kafdeck.Architecture.Tests;

[Collection("W68PostgresLeaseSchema")]
public sealed class V08G3DeclarativePersistenceTests
{
    [Theory]
    [InlineData("create-receipt")]
    [InlineData("adopt-update")]
    [InlineData("atomic-revocation")]
    [InlineData("lease-expiry-during-commit")]
    [InlineData("generation-fence")]
    [InlineData("source-revocation")]
    [InlineData("stale-subscription")]
    [InlineData("principal-holder")]
    [InlineData("competing-apply")]
    [InlineData("relinquish-readopt")]
    [InlineData("source-r2")]
    [InlineData("immutable-plan")]
    [InlineData("schema-replay")]
    public Task Sqlite_production_store_fences_real_transactions(string scenario) => RunAsync(false, scenario);

    [W68G3PostgresTheory]
    [InlineData("create-receipt")]
    [InlineData("adopt-update")]
    [InlineData("atomic-revocation")]
    [InlineData("lease-expiry-during-commit")]
    [InlineData("generation-fence")]
    [InlineData("source-revocation")]
    [InlineData("stale-subscription")]
    [InlineData("principal-holder")]
    [InlineData("competing-apply")]
    [InlineData("relinquish-readopt")]
    [InlineData("source-r2")]
    [InlineData("immutable-plan")]
    [InlineData("schema-replay")]
    public Task PostgreSql_production_store_fences_real_transactions(string scenario) => RunAsync(true, scenario);

    private static async Task RunAsync(bool postgres, string scenario)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        await using var f = await W68G3PersistenceFixture.CreateAsync(postgres, token);
        switch (scenario)
        {
            case "create-receipt":
            {
                var (plan, lease) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                Assert.True(await f.Store.IsPlanCurrentAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Yes, token));
                var receipt = await Apply(f, plan, token);
                Assert.NotNull(receipt); Assert.Equal(1, receipt.SubscriptionRevision); Assert.Equal(1, receipt.OwnershipRevision);
                Assert.Equal(receipt, await Apply(f, plan, token));
                Assert.False(await f.Store.IsPlanCurrentAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Yes, token));
                Assert.Equal(receipt, await f.Store.GetVerifiedCommitReceiptAsync(plan.PlanId, plan.PlanFingerprint,
                    W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token));
                Assert.Equal(receipt, await new W68RuntimeStore(f.Connections, f.Generation).GetCommitReceiptAsync(plan.PlanId, token));
                Assert.Equal(1, (await f.Routing.GetSubscriptionAsync("subscription-a", token))!.Revision);
                var nextOperation = Guid.NewGuid();
                var next = await f.Store.AcquireLeaseAsync(W68G3PersistenceFixture.Deployment, lease.LeaseScopeId,
                    W68G3PersistenceFixture.Controller, W68G3PersistenceFixture.Owner, W68G3PersistenceFixture.Principal,
                    nextOperation.ToString("N"), 30, W68G3PersistenceFixture.Yes, token);
                Assert.NotNull(next); Assert.True(next.Epoch > lease.Epoch);
                Assert.Equal(1L, await Count(f, "kafdeck_declarative_commit_receipts", token));
                Assert.Equal(1L, await Count(f, "kafdeck_declarative_ownership_audit", token));
                break;
            }
            case "adopt-update":
            {
                var original = await f.SeedSubscriptionAsync(token: token);
                var (adopt, _) = await f.PlanAsync(W68RuntimePlanCommand.Adopt, token: token);
                Assert.NotNull(await Apply(f, adopt, token));
                var unchanged = await f.Routing.GetSubscriptionAsync("subscription-a", token); Assert.NotNull(unchanged);
                Assert.Equal(W68RuntimeFingerprints.Subscription(original), W68RuntimeFingerprints.Subscription(unchanged));
                var (update, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var result = await Apply(f, update, token); Assert.NotNull(result);
                Assert.Equal(2, result.SubscriptionRevision); Assert.Equal(2, result.OwnershipRevision);
                var changed = await f.Routing.GetSubscriptionAsync("subscription-a", token); Assert.NotNull(changed);
                Assert.Equal("destination-b", changed.Definition.DestinationId); Assert.Equal(NotificationSubscriptionState.Paused, changed.State);
                Assert.Equal(2L, await Count(f, "kafdeck_declarative_ownership_audit", token));
                break;
            }
            case "atomic-revocation":
            {
                var (plan, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var calls = 0;
                var denied = await f.Store.ApplySubscriptionAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Principal,
                    (_, _) => Task.FromResult(Interlocked.Increment(ref calls) == 1), token);
                Assert.Null(denied); Assert.True(calls >= 2);
                Assert.Null(await f.Routing.GetSubscriptionAsync("subscription-a", token));
                Assert.Null(await f.Store.GetOwnershipAsync(plan.Key, token));
                Assert.Null(await f.Store.GetCommitReceiptAsync(plan.PlanId, token));
                Assert.Equal(0L, await Count(f, "kafdeck_declarative_ownership_audit", token));
                Assert.Equal(0L, await Count(f, "kafdeck_declarative_generation_history", token));
                break;
            }
            case "generation-fence":
            {
                var (committedPlan, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, id: "already-committed", token: token);
                var committedReceipt = await Apply(f, committedPlan, token); Assert.NotNull(committedReceipt);
                var (old, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var before = f.Generation; var after = Guid.NewGuid();
                await W68RuntimeSchema.AdmitGenerationAsync(f.Connections, W68G3PersistenceFixture.Deployment, before, after, W68G3PersistenceFixture.R2, token);
                await W68RuntimeSchema.AdmitGenerationAsync(f.Connections, W68G3PersistenceFixture.Deployment, before, after, W68G3PersistenceFixture.R2, token);
                Assert.Null(await Apply(f, old, token));
                f.Generation = after;
                Assert.Null(await Apply(f, old, token));
                Assert.False(await f.Store.IsPlanCurrentAsync(old.PlanId, old.PlanFingerprint, W68G3PersistenceFixture.Yes, token));
                Assert.True(committedPlan.ExpiresAtUtc > await f.Store.ReadDatabaseUtcNowAsync(token));
                Assert.Null(await Apply(f, committedPlan, token));
                Assert.Null(await f.Store.GetVerifiedCommitReceiptAsync(committedPlan.PlanId, committedPlan.PlanFingerprint,
                    W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token));
                Assert.Equal(committedReceipt, await f.Store.GetCommitReceiptAsync(committedPlan.PlanId, token));
                Assert.Null(await new W68RuntimeStore(f.Connections).AcquireLeaseAsync(W68G3PersistenceFixture.Deployment,
                    W68RuntimeFingerprints.LeaseScope("subscription-a"), W68G3PersistenceFixture.Controller, W68G3PersistenceFixture.Owner,
                    W68G3PersistenceFixture.Principal, Guid.NewGuid().ToString("N"), 30, W68G3PersistenceFixture.Yes, token));
                var (fresh, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                Assert.NotNull(await Apply(f, fresh, token));
                Assert.Null(await f.Store.GetCommitReceiptAsync(old.PlanId, token));
                Assert.Equal(old.PlanFingerprint, (await f.Store.GetPlanAsync(old.PlanId, token))!.PlanFingerprint);
                Assert.Equal(after, await f.Store.GetDeploymentGenerationAsync(W68G3PersistenceFixture.Deployment, token));
                break;
            }
            case "lease-expiry-during-commit":
            {
                var (plan, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, ttlSeconds: 2, token: token);
                var calls = 0;
                var result = await f.Store.ApplySubscriptionAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Principal,
                    async (_, ct) =>
                    {
                        if (Interlocked.Increment(ref calls) == 2) await Task.Delay(TimeSpan.FromMilliseconds(2100), ct);
                        return true;
                    }, token);
                Assert.Null(result); Assert.True(calls >= 2);
                Assert.True(await f.Store.ReadDatabaseUtcNowAsync(token) > plan.ExpiresAtUtc);
                Assert.Null(await f.Routing.GetSubscriptionAsync("subscription-a", token));
                Assert.Null(await f.Store.GetOwnershipAsync(plan.Key, token));
                Assert.Null(await f.Store.GetCommitReceiptAsync(plan.PlanId, token));
                Assert.Equal(0L, await Count(f, "kafdeck_declarative_ownership_audit", token));
                break;
            }
            case "source-revocation":
            {
                var (committedPlan, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, id: "already-committed", token: token);
                var committed = await Apply(f, committedPlan, token); Assert.NotNull(committed);
                var (plan, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var source = await f.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, W68G3PersistenceFixture.Deployment, W68G3PersistenceFixture.Source, token);
                Assert.NotNull(source);
                var disabled = await f.Store.UpsertRegistrationAsync(source with { Enabled = false }, source.Revision, W68G3PersistenceFixture.Yes, token);
                Assert.NotNull(disabled); Assert.Null(await Apply(f, plan, token));
                Assert.False(await f.Store.IsPlanCurrentAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Yes, token));
                Assert.Null(await Apply(f, committedPlan, token));
                Assert.Null(await f.Store.GetVerifiedCommitReceiptAsync(committedPlan.PlanId, committedPlan.PlanFingerprint,
                    W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token));
                Assert.Equal(committed, await f.Store.GetCommitReceiptAsync(committedPlan.PlanId, token));
                Assert.NotNull(await f.Store.UpsertRegistrationAsync(disabled with { Enabled = true }, disabled.Revision, W68G3PersistenceFixture.Yes, token));
                Assert.Null(await Apply(f, plan, token)); // re-enable cannot revive old immutable source revision proof
                Assert.False(await f.Store.IsPlanCurrentAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Yes, token));
                Assert.Null(await f.Store.GetVerifiedCommitReceiptAsync(committedPlan.PlanId, committedPlan.PlanFingerprint,
                    W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token));
                Assert.Null(await f.Routing.GetSubscriptionAsync("subscription-a", token));
                break;
            }
            case "stale-subscription":
            {
                var current = await f.SeedSubscriptionAsync(token: token);
                var (plan, _) = await f.PlanAsync(W68RuntimePlanCommand.Adopt, token: token);
                Assert.True(await f.Store.IsPlanCurrentAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Yes, token));
                Assert.NotNull(await f.Routing.ReplaceSubscriptionAsync(current.Definition, NotificationSubscriptionState.Paused,
                    current.Revision, await f.Store.ReadDatabaseUtcNowAsync(token), token));
                Assert.Null(await Apply(f, plan, token)); Assert.Null(await f.Store.GetOwnershipAsync(plan.Key, token));
                Assert.False(await f.Store.IsPlanCurrentAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Yes, token));
                break;
            }
            case "principal-holder":
            {
                var (plan, lease) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                Assert.Null(await f.Store.ApplySubscriptionAsync(plan.PlanId, plan.PlanFingerprint, "different-authenticated-principal", W68G3PersistenceFixture.Yes, token));
                var other = new W68RuntimeRegistration(W68RuntimeRegistrationKind.Controller, W68G3PersistenceFixture.Deployment,
                    "other-controller", Guid.NewGuid(), 0, W68G3PersistenceFixture.Owner, "other-principal", "", "", "", true);
                Assert.NotNull(await f.Store.UpsertRegistrationAsync(other, 0, W68G3PersistenceFixture.Yes, token));
                Assert.Null(await f.Store.AcquireLeaseAsync(lease.DeploymentId, lease.LeaseScopeId, other.RegistrationId,
                    other.OwnerId, other.PrincipalId, lease.HolderId, 30, W68G3PersistenceFixture.Yes, token));
                Assert.Null(await f.Store.RenewLeaseAsync(lease, other.RegistrationId, other.OwnerId, other.PrincipalId,
                    30, W68G3PersistenceFixture.Yes, token));
                Assert.NotNull(await Apply(f, plan, token));
                break;
            }
            case "competing-apply":
            {
                var (plan, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var other = new W68RuntimeStore(f.Connections, f.Generation);
                var outcomes = await Task.WhenAll(Apply(f, plan, token), other.ApplySubscriptionAsync(plan.PlanId,
                    plan.PlanFingerprint, W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token));
                Assert.All(outcomes, value => Assert.NotNull(value)); Assert.Equal(outcomes[0], outcomes[1]);
                Assert.Equal(1L, await Count(f, "kafdeck_declarative_commit_receipts", token));
                Assert.Equal(1L, await Count(f, "kafdeck_declarative_ownership_audit", token));
                break;
            }
            case "relinquish-readopt":
            {
                var (create, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var first = await Apply(f, create, token); Assert.NotNull(first);
                var (release, _) = await f.PlanAsync(W68RuntimePlanCommand.Relinquish, token: token);
                Assert.NotNull(await Apply(f, release, token));
                Assert.Equal("Relinquished", (await f.Store.GetOwnershipAsync(create.Key, token))!.State);
                var (adopt, _) = await f.PlanAsync(W68RuntimePlanCommand.Adopt, token: token);
                var next = await Apply(f, adopt, token); Assert.NotNull(next);
                Assert.NotEqual(first.OwnershipGeneration, next.OwnershipGeneration); Assert.True(next.OwnershipRevision > first.OwnershipRevision);
                Assert.Equal(1, next.SubscriptionRevision); Assert.Equal(2L, await Count(f, "kafdeck_declarative_generation_history", token));
                break;
            }
            case "source-r2":
            {
                var (create, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                Assert.NotNull(await Apply(f, create, token));
                var (old, lease) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var source = await f.Store.GetRegistrationAsync(W68RuntimeRegistrationKind.Source, W68G3PersistenceFixture.Deployment, W68G3PersistenceFixture.Source, token);
                Assert.NotNull(source);
                Assert.NotNull(await f.Store.UpsertRegistrationAsync(source with { PinnedRevision = W68G3PersistenceFixture.R2 }, source.Revision, W68G3PersistenceFixture.Yes, token));
                Assert.Null(await Apply(f, old, token));
                Assert.True(await f.Store.ReleaseLeaseAsync(lease, W68G3PersistenceFixture.Controller, W68G3PersistenceFixture.Owner,
                    W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token));
                var (fresh, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, sourceRevision: W68G3PersistenceFixture.R2, token: token);
                Assert.Equal(W68G3PersistenceFixture.R1, fresh.ExpectedOwnedSourceRevisionHash);
                Assert.NotNull(await Apply(f, fresh, token));
                Assert.Equal(W68G3PersistenceFixture.R2, (await f.Store.GetOwnershipAsync(fresh.Key, token))!.SourceRevisionHash);
                break;
            }
            case "immutable-plan":
            {
                var (plan, _) = await f.PlanAsync(W68RuntimePlanCommand.Apply, token: token);
                var tampered = plan with { DesiredCanonicalJson = "{\"changed\":true}", DesiredFingerprint = W68RuntimeFingerprints.Desired("{\"changed\":true}") };
                tampered = tampered with { PlanFingerprint = W68RuntimeFingerprints.Plan(tampered) };
                Assert.Null(await f.Store.SavePlanAsync(tampered, W68G3PersistenceFixture.Yes, token));
                Assert.Equal(plan.PlanFingerprint, (await f.Store.LoadPlanAsync(plan.PlanId, token))!.PlanFingerprint);
                Assert.Null(await f.Store.ApplySubscriptionAsync(plan.PlanId, new string('b', 64), W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token));
                break;
            }
            case "schema-replay":
            {
                await W68RuntimeSchema.InstallOrUpgradeAsync(f.Connections, token);
                await f.Store.InitializeAsync(token);
                await using var c = await f.Connections.OpenAsync(token);
                await using var cmd = c.CreateCommand(); cmd.CommandText = "ALTER TABLE kafdeck_declarative_plans ADD COLUMN unreviewed_column TEXT";
                await cmd.ExecuteNonQueryAsync(token);
                await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.InitializeAsync(token));
                await Assert.ThrowsAsync<InvalidOperationException>(() => W68RuntimeSchema.InstallOrUpgradeAsync(f.Connections, token));
                break;
            }
            default: throw new ArgumentException("Unknown closed storage scenario.");
        }
    }

    private static Task<W68RuntimeCommitReceipt?> Apply(W68G3PersistenceFixture fixture, W68StoredRuntimePlan plan, CancellationToken token) =>
        fixture.Store.ApplySubscriptionAsync(plan.PlanId, plan.PlanFingerprint, W68G3PersistenceFixture.Principal, W68G3PersistenceFixture.Yes, token);
    private static async Task<long> Count(W68G3PersistenceFixture f, string table, CancellationToken token)
    {
        await using var c = await f.Connections.OpenAsync(token); await using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM " + table;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(token), System.Globalization.CultureInfo.InvariantCulture);
    }
}
