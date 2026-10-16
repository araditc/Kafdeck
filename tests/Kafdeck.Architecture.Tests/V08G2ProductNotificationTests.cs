using System.Security.Cryptography.X509Certificates;
using System.Security.Claims;
using System.Text.Json;
using Kafdeck.Api;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Npgsql;
using Xunit;
using Xunit.Abstractions;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G2ProductNotificationTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData(AuthorizationAction.NotificationManage, null, null, true, 200)]
    [InlineData(AuthorizationAction.NotificationManage, "*", null, true, 403)]
    [InlineData(AuthorizationAction.NotificationManage, "subscription/one", null, true, 403)]
    [InlineData(AuthorizationAction.NotificationManage, null, "prod", true, 403)]
    [InlineData(AuthorizationAction.NotificationRead, null, null, true, 403)]
    [InlineData(AuthorizationAction.NotificationManage, null, null, false, 401)]
    public async Task Actual_status_endpoint_requires_unconstrained_global_management_before_reading_any_counts(
        AuthorizationAction action, string? resourcePattern, string? clusterScope, bool authenticated, int expectedStatus)
    {
        var policy = new AuthorizationPolicyDefinition(
            [new("status", [new(action, clusterScope is null ? [] : [clusterScope], resourcePattern is null ? [] : [resourcePattern])])],
            [new("https://idp.example", "operator", ["status"])], []);
        var authorization = new KafdeckAuthorizationService(new KafdeckOptions(
                new DeploymentOptions("http://127.0.0.1:0", null, AccessMode.Oidc, null), []),
            new AuthorizationPolicyEvaluator(AuthorizationPolicyCompiler.Compile(policy)));
        var principal = authenticated ? OidcIdentityNormalizer.Normalize(new ClaimsPrincipal(new ClaimsIdentity(
                [new Claim("sub", "operator")], "oidc")), "https://idp.example", null, DateTimeOffset.UtcNow)
            : new ClaimsPrincipal();
        var spy = new StatusReadSpy();
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddSingleton(authorization);
        builder.Services.AddSingleton<INotificationProductEventOutbox>(spy);
        await using var app = builder.Build();
        app.Use((context, next) => { context.User = principal; return next(context); });
        app.MapKafdeckNotificationProductEventStatus();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await app.StartAsync(timeout.Token);
        using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false }) { BaseAddress = new Uri(Assert.Single(app.Urls)) };
        using var response = await client.GetAsync("/api/v1/notifications/product-events/status", timeout.Token);
        Assert.Equal(expectedStatus, (int)response.StatusCode);
        Assert.Equal(expectedStatus == 200 ? 1 : 0, spy.Reads);
        await app.StopAsync(timeout.Token);
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("false", false)]
    [InlineData("true", true)]
    public void Product_source_activation_is_explicit_and_typed(string? value, bool expected)
    {
        var settings = ProductSettings();
        settings["Kafdeck:Notifications:ProductEventsEnabled"] = value;
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        Assert.Equal(expected, NotificationProductEventHosting.ReadActivation(configuration, KafdeckConfigurationLoader.Load(configuration)));
    }

    [Theory]
    [InlineData("sometimes", "true", "true", "Standalone", "Standalone")]
    [InlineData("true", "false", "true", "Standalone", "Standalone")]
    [InlineData("true", "true", "false", "Standalone", "Standalone")]
    [InlineData("true", "true", "true", "HighAvailability", "Standalone")]
    [InlineData("true", "true", "true", "Standalone", "HighAvailability")]
    public void Product_source_rejects_invalid_disabled_dependency_and_HA_activation(string value, string notifications,
        string mutations, string notificationMode, string mutationMode)
    {
        var settings = ProductSettings();
        settings["Kafdeck:Notifications:ProductEventsEnabled"] = value;
        settings["Kafdeck:Notifications:Enabled"] = notifications;
        settings["Kafdeck:Administration:Mutations:Enabled"] = mutations;
        settings["Kafdeck:Notifications:ExecutionMode"] = notificationMode;
        settings["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = mutationMode;
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        Assert.Throws<KafdeckConfigurationException>(() => NotificationProductEventHosting.ReadActivation(configuration,
            KafdeckConfigurationLoader.Load(configuration)));
    }

    [Fact]
    public async Task Disabled_capture_creates_no_outbox_or_hosted_sender()
    {
        await using var fixture = await SqliteFixture.CreateAsync(capture: false);
        var operation = Executing();
        await fixture.Repository.CreateAsync(operation.Snapshot);
        await FinishAsync(fixture.Repository, operation);
        await using var connection = await fixture.Factory.OpenAsync();
        await using var query = connection.CreateCommand();
        query.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='kafdeck_notification_product_outbox'";
        Assert.Null(await query.ExecuteScalarAsync());
        var services = new ServiceCollection();
        services.AddKafdeckNotificationProductEvents(false);
        Assert.DoesNotContain(services, item => item.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task Outbox_failure_rolls_back_terminal_save_and_stale_CAS_emits_nothing()
    {
        await using var fixture = await SqliteFixture.CreateAsync();
        var operation = Executing();
        var original = operation.Snapshot;
        await fixture.Repository.CreateAsync(original);
        await fixture.SqlAsync("""
            CREATE TRIGGER fail_product_capture BEFORE INSERT ON kafdeck_notification_product_outbox
            BEGIN SELECT RAISE(ABORT, 'fixture-outbox-failure'); END
            """);
        operation.Complete(MutationExecutionResultKind.AppliedVerified, "verified", DateTimeOffset.UtcNow);
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Repository.TrySaveAsync(operation.Snapshot, original.Version));
        Assert.Equal(MutationOperationState.Executing, (await fixture.Repository.GetAsync(original.OperationId))!.State);
        Assert.Empty(await fixture.Outbox.ReadPendingAsync(50));
        await fixture.SqlAsync("DROP TRIGGER fail_product_capture");
        Assert.Equal(MutationSaveOutcome.VersionConflict,
            (await fixture.Repository.TrySaveAsync(operation.Snapshot with { Version = original.Version + 2 }, original.Version + 1)).Outcome);
        Assert.Empty(await fixture.Outbox.ReadPendingAsync(50));
        Assert.Equal(MutationSaveOutcome.Saved, (await fixture.Repository.TrySaveAsync(operation.Snapshot, original.Version)).Outcome);
        Assert.Single(await fixture.Outbox.ReadPendingAsync(50));
    }

    [Theory]
    [InlineData(MutationExecutionResultKind.AppliedUnverified)]
    [InlineData(MutationExecutionResultKind.ExecutionUnknown)]
    [InlineData(MutationExecutionResultKind.PartiallyApplied)]
    public async Task Unverified_or_ambiguous_effects_are_not_projected_as_verified_events(MutationExecutionResultKind result)
    {
        await using var fixture = await SqliteFixture.CreateAsync();
        var operation = Executing();
        await fixture.Repository.CreateAsync(operation.Snapshot);
        var version = operation.Snapshot.Version;
        operation.Complete(result, "bounded-result", DateTimeOffset.UtcNow);
        Assert.Equal(MutationSaveOutcome.Saved, (await fixture.Repository.TrySaveAsync(operation.Snapshot, version)).Outcome);
        Assert.Empty(await fixture.Outbox.ReadPendingAsync(50));
    }

    [Fact]
    public async Task Overflow_is_durable_explicit_and_never_rejects_real_mutation_success()
    {
        await using var fixture = await SqliteFixture.CreateAsync(policy: new(maxPending: 1));
        foreach (var operation in new[] { Executing(), Executing() })
        {
            await fixture.Repository.CreateAsync(operation.Snapshot);
            await FinishAsync(fixture.Repository, operation);
            Assert.Equal(MutationOperationState.AppliedVerified, (await fixture.Repository.GetAsync(operation.Snapshot.OperationId))!.State);
        }
        var status = await fixture.Outbox.GetStatusAsync();
        Assert.Equal(1, status.Pending);
        Assert.Equal(1, status.Overflowed);
        Assert.NotNull(status.LastLossAtUtc);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Outbox.ReadPendingAsync(51));
    }

    [Fact]
    public async Task Retention_expiry_reports_loss_while_preserving_verified_operation()
    {
        await using var fixture = await SqliteFixture.CreateAsync(policy: new(retention: TimeSpan.FromSeconds(1)));
        var operation = Executing();
        await fixture.Repository.CreateAsync(operation.Snapshot);
        await FinishAsync(fixture.Repository, operation);
        await Task.Delay(1100);
        var beforeMaintenance = await fixture.Outbox.GetStatusAsync();
        Assert.Equal(1, beforeMaintenance.Pending);
        Assert.Equal(1, beforeMaintenance.ExpiredPending);
        Assert.Equal(0, beforeMaintenance.Expired);
        Assert.Empty(await fixture.Outbox.ReadPendingAsync(50));
        var status = await fixture.Outbox.GetStatusAsync();
        Assert.Equal(0, status.Pending);
        Assert.Equal(0, status.ExpiredPending);
        Assert.Equal(1, status.Expired);
        Assert.NotNull(status.LastLossAtUtc);
        Assert.Equal(MutationOperationState.AppliedVerified, (await fixture.Repository.GetAsync(operation.Snapshot.OperationId))!.State);
    }

    [Fact]
    public async Task Delivery_backlog_backpressure_retains_source_before_any_new_routing_effect()
    {
        await using var fixture = await SqliteFixture.CreateAsync(policy: new(maxPendingDeliveries: 500));
        var connections = new SqliteNotificationDeliveryDbConnectionFactory(Path.Combine(fixture.Directory, "backlog.db"));
        var routing = new AdoNotificationRoutingStore(connections);
        var deliveries = new AdoNotificationDeliveryStore(connections);
        await routing.InitializeAsync();
        await deliveries.InitializeAsync();
        var now = DateTimeOffset.UtcNow;
        await deliveries.CreateOrGetAsync(new(Guid.NewGuid(), "existing", new string('a', 64),
            NotificationDeliveryState.Pending, 0, now, routedProfileRevisionFingerprint: new string('b', 64)), now);
        var operation = Executing();
        await fixture.Repository.CreateAsync(operation.Snapshot);
        await FinishAsync(fixture.Repository, operation);
        var source = Assert.Single(await fixture.Outbox.ReadPendingAsync(50));
        var catalog = new ConfiguredNotificationDestinationProfileCatalog([]);
        var pump = new NotificationProductEventPump(fixture.Outbox,
            new NotificationRoutingCoordinator(routing, deliveries, catalog), fixture.Policy,
            new NotificationProductEventBackpressure(connections, fixture.Policy));
        var result = await pump.RunCycleAsync();
        Assert.Equal(0, result.Acknowledged);
        Assert.Equal(1, result.Unroutable);
        Assert.Single(await fixture.Outbox.ReadPendingAsync(50));
        Assert.Null(await routing.GetEventAsync(source.EventId));
    }

    [Fact]
    public async Task Product_source_host_with_physical_sender_off_routes_bounded_pending_without_transport_activation()
    {
        await using var fixture = await SqliteFixture.CreateAsync();
        var deliveryPath = Path.Combine(fixture.Directory, "sender-off.db");
        var connections = new SqliteNotificationDeliveryDbConnectionFactory(deliveryPath);
        var routing = new AdoNotificationRoutingStore(connections);
        var deliveries = new AdoNotificationDeliveryStore(connections);
        await routing.InitializeAsync();
        await deliveries.InitializeAsync();
        var profile = new NotificationDestinationProfile("sender-off", NotificationProviderKind.Webhook,
            "Queued without physical sender", [NotificationEventClass.Mutation], new Uri("https://hooks.example.org/notify"));
        await routing.CreateSubscriptionAsync(new NotificationSubscriptionDefinition("sender-off-sub", profile.DestinationId,
            [NotificationEventClass.Mutation], ["mutation.state-changed"]), NotificationSubscriptionState.Active, DateTimeOffset.UtcNow);
        var operation = Executing();
        await fixture.Repository.CreateAsync(operation.Snapshot);
        await FinishAsync(fixture.Repository, operation);
        var product = Assert.Single(await fixture.Outbox.ReadPendingAsync(50));

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IMutationDbConnectionFactory>(fixture.Factory);
        builder.Services.AddSingleton<IMutationOperationRepository>(fixture.Repository);
        builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(connections);
        builder.Services.AddSingleton<INotificationRoutingStore>(routing);
        builder.Services.AddSingleton<INotificationDeliveryStore>(deliveries);
        builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(new ConfiguredNotificationDestinationProfileCatalog([profile]));
        builder.Services.AddSingleton(fixture.Policy);
        builder.Services.AddKafdeckWebhookDelivery(new(true, NotificationPersistenceProvider.Sqlite,
            NotificationExecutionMode.Standalone, deliveryPath, null, DestinationProfiles: [profile]));
        builder.Services.AddKafdeckNotificationProductEvents(true);
        using var host = builder.Build();
        Assert.Null(host.Services.GetService<NotificationDeliveryWorker>());
        Assert.Null(host.Services.GetService<INotificationDeliveryDispatcher>());
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await host.StartAsync(timeout.Token);
        NotificationDeliveryRecord? queued;
        while ((queued = await deliveries.GetAsync(product.EventId, profile.DestinationId, timeout.Token)) is null ||
               (await fixture.Outbox.GetStatusAsync(timeout.Token)).Pending != 0)
            await Task.Delay(20, timeout.Token);
        await host.StopAsync(timeout.Token);
        Assert.Equal(NotificationDeliveryState.Pending, queued.Snapshot.State);
        Assert.Equal(0, queued.Snapshot.AttemptCount);
        Assert.Equal(1, (await fixture.Outbox.GetStatusAsync()).Routed);
    }

    [Fact]
    public async Task Real_product_terminal_save_routes_through_subscription_to_physical_TLS_and_durable_receipt()
    {
        await using var fixture = await SqliteFixture.CreateAsync();
        await using var tls = new V08W66HostedWebhookTlsTests.TlsProvider();
        var deliveryPath = Path.Combine(fixture.Directory, "notifications.db");
        var deliveryConnections = new SqliteNotificationDeliveryDbConnectionFactory(deliveryPath);
        var routing = new AdoNotificationRoutingStore(deliveryConnections);
        var deliveries = new AdoNotificationDeliveryStore(deliveryConnections);
        await routing.InitializeAsync();
        await deliveries.InitializeAsync();
        var profile = new NotificationDestinationProfile("product-lab", NotificationProviderKind.Webhook,
            "Product event fixture", [NotificationEventClass.Mutation], new Uri($"https://hooks.example.org:{tls.Port}/notify"));
        var catalog = new ConfiguredNotificationDestinationProfileCatalog([profile]);
        await routing.CreateSubscriptionAsync(new NotificationSubscriptionDefinition("product-subscription", profile.DestinationId,
            [NotificationEventClass.Mutation], ["mutation.state-changed"]), NotificationSubscriptionState.Active, DateTimeOffset.UtcNow);
        var coordinator = new NotificationRoutingCoordinator(routing, deliveries, catalog);
        var operation = Executing();
        await fixture.Repository.CreateAsync(operation.Snapshot);
        await FinishAsync(fixture.Repository, operation);
        var product = Assert.Single(await fixture.Outbox.ReadPendingAsync(50));

        // Simulate interruption after routing but before source acknowledgement.
        await coordinator.RouteAsync(product.Project(), DateTimeOffset.UtcNow);
        Assert.Single(await fixture.Outbox.ReadPendingAsync(50));
        var pump = new NotificationProductEventPump(fixture.Outbox, coordinator, fixture.Policy,
            new NotificationProductEventBackpressure(deliveryConnections, fixture.Policy));
        Assert.Equal(1, (await pump.RunCycleAsync()).Acknowledged);
        Assert.Empty(await fixture.Outbox.ReadPendingAsync(50));

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(catalog);
        builder.Services.AddSingleton<INotificationCredentialResolver>(new NoCredentials());
        builder.Services.AddSingleton<INotificationHostAddressLookup>(tls);
        builder.Services.AddSingleton<INotificationPinnedTcpDialer>(tls);
        builder.Services.AddSingleton(new NotificationPinnedHttpHandlerFactory(tls, NotificationAddressPolicy.NoConfiguredNat64,
            trustRoots: new X509Certificate2Collection(tls.Root)));
        builder.Services.AddSingleton<INotificationRoutingStore>(routing);
        builder.Services.AddSingleton<INotificationDeliveryStore>(deliveries);
        builder.Services.AddKafdeckWebhookDelivery(new(true, NotificationPersistenceProvider.Sqlite,
            NotificationExecutionMode.Standalone, deliveryPath, null, DestinationProfiles: [profile], WebhookDeliveryEnabled: true));
        using var host = builder.Build();
        await host.StartAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var request = await tls.Received.Task.WaitAsync(timeout.Token);
        NotificationDeliveryRecord? receipt;
        while ((receipt = await deliveries.GetAsync(product.EventId, profile.DestinationId, timeout.Token))?.Snapshot.State != NotificationDeliveryState.Delivered)
            await Task.Delay(20, timeout.Token);
        await host.Services.GetRequiredService<NotificationDeliveryWorker>().RunDueCycleAsync(timeout.Token);
        await host.StopAsync(timeout.Token);
        Assert.Equal(1, tls.RequestCount);
        Assert.Equal(1, receipt.Snapshot.AttemptCount);
        Assert.Equal(0, (await fixture.Outbox.GetStatusAsync()).Pending);
        Assert.DoesNotContain("PRODUCT_SECRET_SENTINEL", request, StringComparison.Ordinal);
        Assert.DoesNotContain(operation.Snapshot.OperationId.ToString("D"), request, StringComparison.Ordinal);
        using var body = JsonDocument.Parse(request[(request.IndexOf("\r\n\r\n", StringComparison.Ordinal) + 4)..]);
        Assert.Equal("mutation.state-changed", body.RootElement.GetProperty("eventType").GetString());
        Assert.Equal(product.EventId, body.RootElement.GetProperty("eventId").GetGuid());
        var reopened = new AdoNotificationDeliveryStore(new SqliteNotificationDeliveryDbConnectionFactory(deliveryPath));
        Assert.Equal(NotificationDeliveryState.Delivered, (await reopened.GetAsync(product.EventId, profile.DestinationId))!.Snapshot.State);
        output.WriteLine("PRODUCT_EVENT_TO_REAL_TLS terminal=AppliedVerified; atomicOutbox=true; routeBeforeAckReplay=true; subscription=true; HTTP=204; ledger=Delivered; attempts=1; providerPOSTs=1; rawIntentLeaks=0; KafkaEffectNotPartOfThisFixture=true");
        await WriteEvidenceAsync("product-source-tls-evidence.json", new
        {
            observedAtUtc = DateTimeOffset.UtcNow, sourceState = "AppliedVerified", atomicOutbox = true,
            routeBeforeAckReplay = true, activeSubscription = true, physicalHttpStatus = 204,
            durableDeliveryState = "Delivered", durableAttemptCount = receipt.Snapshot.AttemptCount,
            physicalProviderPosts = tls.RequestCount, rawIntentLeaks = 0,
            kafkaEffectPartOfThisFixture = false, physicalSenderHostHaAccepted = false
        });
    }

    [G2PostgreSqlFact]
    public async Task PostgreSql_product_outbox_is_atomic_unique_under_concurrent_terminal_CAS()
    {
        var configured = Environment.GetEnvironmentVariable("KAFDECK_TEST_POSTGRES")
            ?? throw new InvalidOperationException("A real PostgreSQL fixture is required.");
        // Npgsql intentionally removes password material from ConnectionString
        // after Open. Retain the original fixture settings privately for each
        // independent connection instead of copying the opened connection.
        var settings = new NpgsqlConnectionStringBuilder(configured) { Pooling = false };
        var connection = new NpgsqlConnection(settings.ConnectionString);
        await using var admin = connection;
        await admin.OpenAsync();
        var schema = "g2_product_" + Guid.NewGuid().ToString("N");
        await using var command = admin.CreateCommand();
        command.CommandText = $"CREATE SCHEMA \"{schema}\"";
        await command.ExecuteNonQueryAsync();
        try
        {
            settings.SearchPath = schema;
            var factory = new PostgreSqlMutationDbConnectionFactory(settings.ConnectionString);
            var left = new AdoMutationOperationRepository(factory, captureProductNotifications: true);
            var right = new AdoMutationOperationRepository(factory, captureProductNotifications: true);
            await left.InitializeAsync();
            await right.InitializeAsync();
            var operation = Executing();
            await left.CreateAsync(operation.Snapshot);
            var version = operation.Snapshot.Version;
            operation.Complete(MutationExecutionResultKind.AppliedVerified, "verified", DateTimeOffset.UtcNow);
            var saves = await Task.WhenAll(left.TrySaveAsync(operation.Snapshot, version), right.TrySaveAsync(operation.Snapshot, version));
            Assert.Single(saves, item => item.Outcome == MutationSaveOutcome.Saved);
            var outbox = new MutationNotificationOutbox(factory);
            var pending = Assert.Single(await outbox.ReadPendingAsync(50));
            Assert.True(await outbox.AcknowledgeAsync(pending.EventId));
            Assert.False(await outbox.AcknowledgeAsync(pending.EventId));
            Assert.Equal(1, (await outbox.GetStatusAsync()).Routed);
            await WriteEvidenceAsync("postgres-product-outbox-evidence.json", new
            {
                observedAtUtc = DateTimeOffset.UtcNow, postgresVersion = admin.ServerVersion,
                concurrentTerminalSaves = 2, savedCount = saves.Count(item => item.Outcome == MutationSaveOutcome.Saved),
                capturedOutboxRows = 1, firstAcknowledgementAccepted = true,
                duplicateAcknowledgementAccepted = false, durableRoutedCount = 1,
                physicalKafkaEffectPartOfThisFixture = false
            });
        }
        finally
        {
            command.CommandText = $"DROP SCHEMA \"{schema}\" CASCADE";
            await command.ExecuteNonQueryAsync();
        }
    }

    private static MutationOperation Executing()
    {
        var now = DateTimeOffset.UtcNow;
        var intent = new MutationIntentDescriptor(MutationOperationKind.TopicCreate, "lab", "{\"opaque\":\"PRODUCT_SECRET_SENTINEL\"}",
            ["cluster/lab/topic/product"], [new MutationPrecondition("topic", "absent")]);
        var operation = MutationOperation.CreatePreview("oidc:https://idp.example|operator", intent,
            MutationRiskClassifier.Classify(new(MutationOperationKind.TopicCreate)), "g2-product-fixture", now.AddMinutes(5), now,
            Guid.NewGuid().ToString("N"));
        operation.OpenForConfirmation(now);
        operation.Confirm(operation.Snapshot.RequesterPrincipalId, operation.Snapshot.PreviewHash, now);
        operation.ClaimExecution(now, now.AddMinutes(2));
        operation.MarkDispatchStarted(now);
        return operation;
    }

    private static Dictionary<string, string?> ProductSettings() => new()
    {
        ["Kafdeck:Notifications:Enabled"] = "true",
        ["Kafdeck:Notifications:ExecutionMode"] = "Standalone",
        ["Kafdeck:Administration:Mutations:Enabled"] = "true",
        ["Kafdeck:Administration:Mutations:Persistence:ExecutionMode"] = "Standalone"
    };

    private static async Task FinishAsync(AdoMutationOperationRepository repository, MutationOperation operation)
    {
        var version = operation.Snapshot.Version;
        operation.Complete(MutationExecutionResultKind.AppliedVerified, "verified", DateTimeOffset.UtcNow);
        Assert.Equal(MutationSaveOutcome.Saved, (await repository.TrySaveAsync(operation.Snapshot, version)).Outcome);
    }

    private static async Task WriteEvidenceAsync(string name, object value)
    {
        var directory = Environment.GetEnvironmentVariable("KAFDECK_G2_EVIDENCE_DIRECTORY");
        if (string.IsNullOrWhiteSpace(directory))
        {
            DirectoryInfo? root = new DirectoryInfo(AppContext.BaseDirectory);
            while (root is not null && !File.Exists(Path.Combine(root.FullName, "Kafdeck.slnx"))) root = root.Parent;
            if (root is null) throw new InvalidOperationException("Could not find repository evidence directory.");
            directory = Path.Combine(root.FullName, "artifacts", "g2");
        }
        Directory.CreateDirectory(directory);
        await File.WriteAllTextAsync(Path.Combine(directory, name),
            JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true }));
    }

    private sealed class NoCredentials : INotificationCredentialResolver
    {
        public ValueTask<NotificationCredentialValue> ResolveAsync(NotificationCredentialResolutionRequest request,
            CancellationToken cancellationToken) => throw new InvalidOperationException("Anonymous fixture must not resolve a credential.");
    }

    private sealed class StatusReadSpy : INotificationProductEventOutbox
    {
        public int Reads { get; private set; }
        public Task<NotificationProductEventSourceStatus> GetStatusAsync(CancellationToken cancellationToken = default)
        {
            Reads++;
            return Task.FromResult(new NotificationProductEventSourceStatus(3, 17, 0, 0, null));
        }
        public Task<IReadOnlyList<NotificationProductEvent>> ReadPendingAsync(int maxResults, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Status must not read product events.");
        public Task<bool> AcknowledgeAsync(Guid eventId, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("Status must not acknowledge product events.");
    }

    private sealed class SqliteFixture(string directory, IMutationDbConnectionFactory factory,
        AdoMutationOperationRepository repository, NotificationProductEventPolicy policy) : IAsyncDisposable
    {
        public string Directory { get; } = directory;
        public IMutationDbConnectionFactory Factory { get; } = factory;
        public AdoMutationOperationRepository Repository { get; } = repository;
        public NotificationProductEventPolicy Policy { get; } = policy;
        public MutationNotificationOutbox Outbox { get; } = new(factory, policy);
        public static async Task<SqliteFixture> CreateAsync(bool capture = true, NotificationProductEventPolicy? policy = null)
        {
            var directory = Path.Combine(Path.GetTempPath(), "kafdeck-product-" + Guid.NewGuid().ToString("N"));
            System.IO.Directory.CreateDirectory(directory);
            var factory = new SqliteMutationDbConnectionFactory(Path.Combine(directory, "mutation.db"));
            var actualPolicy = policy ?? new();
            var repository = new AdoMutationOperationRepository(factory, captureProductNotifications: capture,
                notificationProductPolicy: actualPolicy);
            await repository.InitializeAsync();
            return new(directory, factory, repository, actualPolicy);
        }
        public async Task SqlAsync(string sql)
        {
            await using var connection = await Factory.OpenAsync();
            await using var command = connection.CreateCommand(); command.CommandText = sql; await command.ExecuteNonQueryAsync();
        }
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools();
            var target = Path.GetFullPath(Directory);
            var root = Path.GetFullPath(Path.GetTempPath()).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
            if (!target.StartsWith(root, StringComparison.OrdinalIgnoreCase) ||
                !Path.GetFileName(target).StartsWith("kafdeck-product-", StringComparison.Ordinal))
                throw new InvalidOperationException("Product fixture cleanup target escaped its intended temporary directory.");
            System.IO.Directory.Delete(target, recursive: true);
            return ValueTask.CompletedTask;
        }
    }
}
