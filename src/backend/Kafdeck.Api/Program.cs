using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kafdeck.Api;
using Kafdeck.Core;
using Kafdeck.Core.Catalog;
using Kafdeck.Core.Governance;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Consumers;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Notifications;
using Kafdeck.Core.Observability;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Schemas;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Infrastructure.Ecosystem;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Infrastructure.SchemaRegistry;
using Kafdeck.Infrastructure.SerDe;
using Kafdeck.Infrastructure.Security;
using Kafdeck.Modules.Clusters;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Connect;
using Kafdeck.Modules.Consumers;
using Kafdeck.Modules.Generator;
using Kafdeck.Modules.Records;
using Kafdeck.Modules.Schemas;
using Kafdeck.Modules.Topics;

Activity.DefaultIdFormat = ActivityIdFormat.W3C;
Activity.ForceDefaultIdFormat = true;

var declarativeSchemaMigration = args.Contains("--declarative-schema-migrate", StringComparer.Ordinal);
var declarativeSchemaAdoption = args.Contains("--declarative-adopt-existing", StringComparer.Ordinal);
var declarativeGenerationAdmission = args.Contains("--declarative-generation-admit", StringComparer.Ordinal);
var governanceGenerationAdmission = args.Contains("--governance-generation-admit", StringComparer.Ordinal);
var issueSetupCode = args.Contains("--issue-setup-code", StringComparer.Ordinal);
if (issueSetupCode && (governanceGenerationAdmission || declarativeSchemaMigration || declarativeSchemaAdoption || declarativeGenerationAdmission))
    throw new InvalidOperationException("Setup code generation is a separate local command.");
if (governanceGenerationAdmission && (declarativeGenerationAdmission || declarativeSchemaMigration || declarativeSchemaAdoption))
    throw new InvalidOperationException("Governance generation admission is a separate quarantined operator action.");
if (declarativeGenerationAdmission && declarativeSchemaMigration)
    throw new InvalidOperationException("Schema migration and generation admission are separate operator actions.");
if (declarativeSchemaAdoption && !declarativeSchemaMigration)
    throw new InvalidOperationException("Declarative schema adoption requires explicit migration mode.");
var builder = WebApplication.CreateBuilder(args.Where(value => value is not
    ("--declarative-schema-migrate" or "--declarative-adopt-existing" or "--declarative-generation-admit" or "--governance-generation-admit" or "--issue-setup-code")).ToArray());

var kafdeckOptions = KafdeckConfigurationLoader.Load(builder.Configuration);
KafdeckConfigurationValidator.ValidateAndThrow(kafdeckOptions);
var managedIdentityOptions = builder.Configuration.GetSection("Kafdeck:ManagedIdentity").Get<ManagedIdentityOptions>() ?? new();
var managedIdentity = managedIdentityOptions.Enabled ? managedIdentityOptions.ValidateAndCreate(kafdeckOptions) : null;
if (issueSetupCode)
{
    if (managedIdentity is null) throw new InvalidOperationException("Managed identity is disabled.");
    Console.WriteLine(managedIdentity.IssueCode());
    return;
}
if (managedIdentity is not null)
{
    if (builder.Configuration.GetSection("Kafdeck:Authorization:SubjectBindings").Exists() ||
        builder.Configuration.GetSection("Kafdeck:Authorization:GroupBindings").Exists())
        throw new KafdeckConfigurationException("Managed identity and externally configured operator bindings are separate modes.");
    builder.Services.AddSingleton(managedIdentity);
    if (builder.Configuration.GetValue<bool>("Kafdeck:ConnectionWorkspace:Enabled"))
    {
        builder.Services.AddHttpContextAccessor();
        builder.Services.AddSingleton(services => new RegisteredKafkaRuntime(
            services.GetRequiredService<IHttpContextAccessor>(), managedIdentity, services.GetRequiredService<IConfiguration>()));
        builder.Services.AddSingleton(services => new RegisteredHttpServiceRuntime(managedIdentity,
            services.GetRequiredService<IHttpContextAccessor>(),
            services.GetRequiredService<Microsoft.AspNetCore.DataProtection.IDataProtectionProvider>(),
            services.GetRequiredService<IConfiguration>()));
    }
    builder.Services.AddSingleton(_ => new ManagedKeycloakUsers(kafdeckOptions.Deployment.Oidc!, managedIdentityOptions));
}
var mutationOptions = kafdeckOptions.Administration?.Mutations;
var governanceOptions = builder.Configuration.GetSection("Kafdeck:Governance").Get<GovernanceRuntimeOptions>() ?? new();
governanceOptions.Validate(kafdeckOptions.Deployment.Mode == AccessMode.Oidc, mutationOptions?.Enabled == true, governanceGenerationAdmission);
builder.Services.AddSingleton(governanceOptions);
var attributionOptions=builder.Configuration.GetSection("Kafdeck:Governance:Attribution").Get<AttributionRuntimeOptions>()??new();
attributionOptions.Validate();
var observabilityOptions =
    ObservabilityOptions.Effective(kafdeckOptions);
var historicalMetricsOptions =
    observabilityOptions.History;
var dataQualityOptions =
    kafdeckOptions.DataQuality;
var notificationReadOptions =
    kafdeckOptions.Notifications;
var declarativeOptions = builder.Configuration.GetSection("Kafdeck:Declarative").Get<DeclarativeRuntimeOptions>() ?? new();
if (declarativeOptions.Enabled && (kafdeckOptions.Deployment.Mode != AccessMode.Oidc ||
    mutationOptions?.Enabled != true || notificationReadOptions is not { Enabled: true, ManagementEnabled: true }))
    throw new KafdeckConfigurationException("Declarative runtime requires OIDC, governed mutations and notification management.");
if (declarativeOptions.Enabled && declarativeOptions.DeploymentGeneration == Guid.Empty)
    throw new KafdeckConfigurationException("Declarative runtime requires an explicit outside deployment generation.");
if (declarativeOptions.Enabled && !declarativeSchemaMigration && !declarativeGenerationAdmission &&
    !DeclarativeRecoveryAnchor.Allows(declarativeOptions))
    throw new KafdeckConfigurationException("Declarative runtime requires a pinned active outside recovery authority.");
var notificationGovernance = NotificationGovernanceOptions.Read(builder.Configuration, kafdeckOptions);
var productEventsEnabled = NotificationProductEventHosting.ReadActivation(builder.Configuration, kafdeckOptions);
var notificationRecovery = NotificationRecoveryOptions.Read(builder.Configuration, kafdeckOptions, productEventsEnabled);
var maskingPolicy = RecordMaskingPolicyCompiler.Compile(
    kafdeckOptions.Records?.MaskingPolicy ??
    new RecordMaskingPolicyDefinition("default", 1));

var allowedHosts = DeploymentHostPolicy.BuildAllowedHosts(kafdeckOptions.Deployment);
builder.Configuration["AllowedHosts"] = string.Join(';', allowedHosts);
builder.WebHost.UseUrls(kafdeckOptions.Deployment.ListenUrls.ToArray());

var secretResolver = new SecretResolver();

var deploymentAccessToken =
    DeploymentAccessModePolicy.UsesDeploymentToken(kafdeckOptions.Deployment.Mode) &&
    kafdeckOptions.Deployment.AccessToken is not null
        ? secretResolver.Resolve(kafdeckOptions.Deployment.AccessToken).Reveal()
        : null;

var prometheusScrapeToken =
    observabilityOptions.Prometheus.Enabled &&
    observabilityOptions.Prometheus.AccessToken is not null
        ? secretResolver
            .Resolve(observabilityOptions.Prometheus.AccessToken)
            .Reveal()
        : null;

var oidcClientSecret =
    kafdeckOptions.Deployment.Mode == AccessMode.Oidc &&
    kafdeckOptions.Deployment.Oidc?.ClientSecret is not null
        ? secretResolver
            .Resolve(kafdeckOptions.Deployment.Oidc.ClientSecret)
            .Reveal()
        : null;

var otlpHeaders =
    observabilityOptions.Otlp.Enabled &&
    observabilityOptions.Otlp.Headers is not null
        ? secretResolver
            .Resolve(observabilityOptions.Otlp.Headers)
            .Reveal()
        : null;

var historicalMetricsConnectionString =
    historicalMetricsOptions?.Enabled == true &&
    historicalMetricsOptions.Provider ==
        HistoricalMetricsProvider.PostgreSql &&
    historicalMetricsOptions.ConnectionString is not null
        ? secretResolver
            .Resolve(
                historicalMetricsOptions.ConnectionString)
            .Reveal()
        : null;

var dataQualityConnectionString =
    dataQualityOptions?.Enabled == true &&
    dataQualityOptions.Provider ==
        DataQualityPersistenceProvider.PostgreSql &&
    dataQualityOptions.ConnectionString is not null
        ? secretResolver
            .Resolve(
                dataQualityOptions.ConnectionString)
            .Reveal()
        : null;

var notificationConnectionString =
    notificationReadOptions?.Enabled == true &&
    notificationReadOptions.Provider ==
        NotificationPersistenceProvider.PostgreSql &&
    notificationReadOptions.ConnectionString is not null
        ? secretResolver
            .Resolve(notificationReadOptions.ConnectionString)
            .Reveal()
        : null;

ObservabilityStartupSecurity
    .ValidateResolvedCredentialIsolation(
        deploymentAccessToken,
        prometheusScrapeToken,
        otlpHeaders,
        oidcClientSecret);

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    builder.Services.AddKafdeckOidc(
        kafdeckOptions.Deployment,
        secretResolver,
        oidcClientSecret);
    builder.Services.AddKafdeckAntiforgery(kafdeckOptions.Deployment.ListenUrl);
}

builder.Services.AddProblemDetails();
builder.Services.AddKafdeckNonBrowserReads(builder.Configuration);
builder.Services.AddKafdeckOpenTelemetry(
    observabilityOptions,
    otlpHeaders);
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
});

builder.Services.AddSingleton(kafdeckOptions);
builder.Services.AddSingleton(secretResolver);
builder.Services.AddSingleton(maskingPolicy);
builder.Services.AddSingleton<IRecordMaskingPolicyProvider>(new StaticRecordMaskingPolicyProvider(maskingPolicy));
var authorizationPolicies = new ReloadingAuthorizationPolicy(builder.Configuration);
var authorizationPolicy = authorizationPolicies.Current;
builder.Services.AddSingleton(authorizationPolicies);
builder.Services.AddSingleton(authorizationPolicy);
builder.Services.AddSingleton(new AuthorizationPolicyEvaluator(authorizationPolicy, () => authorizationPolicies.Current));
builder.Services.AddSingleton(services => new KafdeckAuthorizationService(
    kafdeckOptions, services.GetRequiredService<AuthorizationPolicyEvaluator>(), services.GetService<NonBrowserReadRegistry>())
    { ManagedIdentity = managedIdentity, RegisteredHttpServices = services.GetService<RegisteredHttpServiceRuntime>() });
builder.Services.AddSingleton<ISecurityAuditSink, LoggingSecurityAuditSink>();
builder.Services.AddSingleton<ApiTelemetry>();
builder.Services.AddSingleton<IKafdeckOperationalTelemetry>(
    services =>
        services.GetRequiredService<ApiTelemetry>());
builder.Services.AddSingleton<KafkaSnapshotPolicy>();
builder.Services.AddSingleton<KafkaSnapshotCoordinator>(services =>
    new KafkaSnapshotCoordinator(services.GetRequiredService<KafkaSnapshotPolicy>(),
        snapshotScope: services.GetService<RegisteredKafkaRuntime>() is { } runtime ? runtime.SnapshotScope : null));
static Func<string, Confluent.Kafka.IAdminClient?>? RegisteredKafkaClient(IServiceProvider services) =>
    services.GetService<RegisteredKafkaRuntime>() is { } runtime ? runtime.GetClient : null;
builder.Services.AddSingleton<IKafkaAdministrationPort>(services =>
    new TelemetryKafkaAdministrationPort(
        new ConfluentKafkaAdministrationAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredClient: RegisteredKafkaClient(services)),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<IKafkaRecordReadPort>(services =>
    new TelemetryKafkaRecordReadPort(
        new ConfluentKafkaRecordReadAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredConsumerConfig: services.GetService<RegisteredKafkaRuntime>() is { } runtime ? runtime.GetConsumerConfig : null),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<ConfluentKafkaConsumerGroupReadAdapter>(
    services =>
        new ConfluentKafkaConsumerGroupReadAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredClient: RegisteredKafkaClient(services)));
builder.Services.AddSingleton<IConsumerGroupSamplingReadPort>(
    services =>
        services.GetRequiredService<
            ConfluentKafkaConsumerGroupReadAdapter>());
builder.Services.AddSingleton<IConsumerGroupReadPort>(services =>
    new TelemetryConsumerGroupReadPort(
        services.GetRequiredService<
            ConfluentKafkaConsumerGroupReadAdapter>(),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<ConsumerExplorerService>();

if (historicalMetricsOptions?.Enabled == true)
{
    builder.Services.AddSingleton<IHistoricalMetricsDbConnectionFactory>(
        _ =>
            historicalMetricsOptions.Provider switch
            {
                HistoricalMetricsProvider.Sqlite =>
                    new SqliteHistoricalMetricsDbConnectionFactory(
                        historicalMetricsOptions.SqliteDatabasePath!),
                HistoricalMetricsProvider.PostgreSql =>
                    new PostgreSqlHistoricalMetricsDbConnectionFactory(
                        historicalMetricsConnectionString!),
                _ => throw new KafdeckConfigurationException(
                    "Historical metrics provider is unsupported."),
            });

    builder.Services.AddSingleton(
        new HistoricalMetricStorePolicy(
            TimeSpan.FromHours(
                historicalMetricsOptions.MaxQueryRangeHours),
            historicalMetricsOptions.MaxSeriesPerQuery,
            historicalMetricsOptions.MaxPointsPerQuery,
            TimeSpan.FromSeconds(
                historicalMetricsOptions.MaxQueryDurationSeconds),
            historicalMetricsOptions.MaxConcurrentQueries));

    builder.Services.AddSingleton<IHistoricalMetricStore>(
        services =>
            new AdoHistoricalMetricStore(
                services.GetRequiredService<
                    IHistoricalMetricsDbConnectionFactory>(),
                services.GetRequiredService<
                    HistoricalMetricStorePolicy>()));

    builder.Services.AddSingleton(
        new HistoricalMetricMaintenancePolicy(
            TimeSpan.FromHours(
                historicalMetricsOptions.RawRetentionHours),
            TimeSpan.FromDays(
                historicalMetricsOptions.RollupRetentionDays),
            HistoricalMetricMaintenancePolicy
                .DefaultRollupResolutionSeconds,
            HistoricalMetricMaintenancePolicy
                .DefaultMaxRollupWindowsPerCycle,
            HistoricalMetricMaintenancePolicy
                .DefaultMaxRollupDeletesPerCycle,
            HistoricalMetricMaintenancePolicy
                .DefaultLeaseDuration,
            HistoricalMetricMaintenancePolicy
                .DefaultCycleInterval,
            HistoricalMetricMaintenancePolicy
                .DefaultMaxCycleDuration));

    builder.Services.AddSingleton<
        AdoHistoricalMetricMaintenanceStore>(
        services =>
            new AdoHistoricalMetricMaintenanceStore(
                services.GetRequiredService<
                    IHistoricalMetricsDbConnectionFactory>()));
    builder.Services.AddSingleton<
        IHistoricalMetricMaintenanceStore>(
        services =>
            services.GetRequiredService<
                AdoHistoricalMetricMaintenanceStore>());
    builder.Services.AddSingleton<
        IHistoricalMetricSamplingLeaseStore>(
        services =>
            services.GetRequiredService<
                AdoHistoricalMetricMaintenanceStore>());

    builder.Services.AddHostedService<
        HistoricalMetricMaintenanceHostedService>();

    builder.Services.AddSingleton<IHistoryObservationPort>(
        services =>
            new HistoricalConsumerHistoryObservationPort(
                services.GetRequiredService<
                    IHistoricalMetricStore>(),
                services.GetRequiredService<
                    HistoricalMetricStorePolicy>()));
    builder.Services.AddSingleton(
        ConsumerLagHistorySamplingPolicy.Default);
    builder.Services.AddHostedService<
        ConsumerLagHistorySamplingHostedService>();
}
else
{
    builder.Services.AddSingleton<IHistoryObservationPort,
        UnavailableHistoryObservationPort>();
}

if (dataQualityOptions?.Enabled == true)
{
    builder.Services.AddSingleton<IDataQualityDbConnectionFactory>(
        _ =>
            dataQualityOptions.Provider switch
            {
                DataQualityPersistenceProvider.Sqlite =>
                    new SqliteDataQualityDbConnectionFactory(
                        dataQualityOptions.SqliteDatabasePath!),
                DataQualityPersistenceProvider.PostgreSql =>
                    new PostgreSqlDataQualityDbConnectionFactory(
                        dataQualityConnectionString!),
                _ => throw new KafdeckConfigurationException(
                    "Data-quality persistence provider is unsupported."),
            });

    builder.Services.AddSingleton<IDataQualityLifecycleStore>(
        services =>
            new AdoDataQualityLifecycleStore(
                services.GetRequiredService<
                    IDataQualityDbConnectionFactory>()));
}

if (notificationReadOptions?.Enabled == true)
{
    // Server-owned, exact-case destination catalog. Physical sending requires
    // the separate, default-off WebhookDeliveryEnabled admission below.
    builder.Services.AddSingleton<INotificationDestinationProfileCatalog>(
        _ => new ConfiguredNotificationDestinationProfileCatalog(
            notificationReadOptions.DestinationProfiles ??
            Array.Empty<NotificationDestinationProfile>()));

    // Only preprovisioned, revision-bound credential REFERENCES are mapped.
    // Registration never reads secret material. The standalone Webhook host
    // admits only an explicit authentication mode and exact credential binding.
    builder.Services.AddSingleton<INotificationCredentialResolver>(services =>
    {
        var profiles = notificationReadOptions.DestinationProfiles ??
            Array.Empty<NotificationDestinationProfile>();
        var byId = profiles.ToDictionary(
            profile => profile.DestinationId, StringComparer.Ordinal);
        var configured = notificationReadOptions.CredentialBindings ??
            Array.Empty<NotificationCredentialReferenceOptions>();
        var approved = configured.Select(reference =>
        {
            if (!byId.TryGetValue(reference.DestinationId, out var profile) ||
                profile.CredentialBindingId is null ||
                !string.Equals(profile.CredentialBindingId.Value,
                    reference.BindingId.Value, StringComparison.Ordinal))
                throw new KafdeckConfigurationException(
                    "Notification credential binding is not admitted by the destination profile.");
            return new ConfiguredNotificationCredentialBinding(
                profile.DestinationId, profile.Provider,
                profile.RevisionFingerprint, reference.BindingId,
                reference.Secret, reference.Enabled);
        }).ToArray();
        return new ConfiguredNotificationCredentialResolver(
            approved, services.GetRequiredService<SecretResolver>());
    });

    builder.Services.AddSingleton<INotificationDeliveryDbConnectionFactory>(
        _ =>
            notificationReadOptions.Provider switch
            {
                NotificationPersistenceProvider.Sqlite =>
                    new SqliteNotificationDeliveryDbConnectionFactory(
                        notificationReadOptions.SqliteDatabasePath!),
                NotificationPersistenceProvider.PostgreSql =>
                    new PostgreSqlNotificationDeliveryDbConnectionFactory(
                        notificationConnectionString!),
                _ => throw new KafdeckConfigurationException(
                    "Notification persistence provider is unsupported."),
            });
    builder.Services.AddSingleton<INotificationRoutingStore>(
        services =>
            new AdoNotificationRoutingStore(
                services.GetRequiredService<
                    INotificationDeliveryDbConnectionFactory>()));
    builder.Services.AddSingleton<AdoNotificationDeliveryStore>(
        services =>
            new AdoNotificationDeliveryStore(
                services.GetRequiredService<
                    INotificationDeliveryDbConnectionFactory>()));
    builder.Services.AddSingleton<INotificationDeliveryStore>(
        services => services.GetRequiredService<AdoNotificationDeliveryStore>());
    builder.Services.AddSingleton<INotificationDeliveryHistoryReader>(
        services => services.GetRequiredService<AdoNotificationDeliveryStore>());
    builder.Services.AddKafdeckWebhookDelivery(notificationReadOptions);
}

builder.Services.AddSingleton<IMetricsObservationPort,
    UnavailableMetricsObservationPort>();
builder.Services.AddSingleton<ConsumerDiagnosticsService>();
builder.Services.AddSingleton<OperationalAnalyticsRuntimeService>();
builder.Services.AddSingleton<IOperationalAnalyticsObservationPort>(
    services =>
        services.GetRequiredService<
            OperationalAnalyticsRuntimeService>());
builder.Services.AddSingleton<OperationalTrendService>(
    services =>
        new OperationalTrendService(
            services.GetService<
                IHistoricalMetricStore>(),
            services.GetService<
                HistoricalMetricStorePolicy>()));
builder.Services.AddSingleton<IRecordSchemaReadPort>(services =>
    new TelemetryRecordSchemaReadPort(
        new ConfluentSchemaRegistryReadAdapter(
            kafdeckOptions.Clusters,
            secretResolver),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<ISchemaCatalogReadPort>(services =>
    new TelemetrySchemaCatalogReadPort(
        new ConfluentSchemaCatalogReadAdapter(
            kafdeckOptions.Clusters,
            secretResolver,
            registeredClient: services.GetService<RegisteredHttpServiceRuntime>() is { } registered ? registered.OpenRegistry : null),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<SchemaDiffService>();
builder.Services.AddSingleton<SchemaExplorerService>();
builder.Services.AddSingleton<SchemaDeveloperService>();
builder.Services.AddSingleton<IRecordDecodePort>(services =>
    new ConfluentRecordDecoder(services.GetRequiredService<IRecordSchemaReadPort>()));
builder.Services.AddSingleton<RecordFilterService>();
builder.Services.AddSingleton<RecordLiveTailService>();
builder.Services.AddSingleton<RecordMaskingService>();
builder.Services.AddSingleton<RecordExportService>();
builder.Services.AddSingleton<IControlledSerdePort, ControlledSerdeService>();
builder.Services.AddSingleton<ClusterExplorerService>();
builder.Services.AddSingleton<TopicExplorerService>();
builder.Services.AddSingleton<IConnectReadPort>(services =>
    new TelemetryConnectReadPort(
        new KafkaConnectReadAdapter(
            kafdeckOptions.Clusters,
            secretResolver,
            registeredClient: services.GetService<RegisteredHttpServiceRuntime>() is { } registered ? registered.OpenConnect : null),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<IKsqlMetadataReadPort>(services =>
    new TelemetryKsqlMetadataReadPort(
        new KsqlDbMetadataReadAdapter(
            kafdeckOptions.Clusters,
            secretResolver,
            registeredClient: services.GetService<RegisteredHttpServiceRuntime>() is { } registered ? registered.OpenKsql : null),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<IKsqlQueryPort>(services =>
    new TelemetryKsqlQueryPort(
        new KsqlDbQueryAdapter(
            kafdeckOptions.Clusters,
            secretResolver,
            registeredClient: services.GetService<RegisteredHttpServiceRuntime>() is { } registered ? registered.OpenKsql : null),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<IStreamsTelemetryReadPort>(services =>
    new TelemetryStreamsTelemetryReadPort(
        new StreamsTelemetryReadAdapter(
            kafdeckOptions.Clusters,
            secretResolver),
        services.GetRequiredService<IKafdeckOperationalTelemetry>()));
builder.Services.AddSingleton<ILineageReadPort, StreamsLineageReadService>();
builder.Services.AddSingleton<ITopicCatalogProvider>(_ =>
    new ConfigurationTopicCatalogProvider(kafdeckOptions));

if (mutationOptions?.Enabled == true)
{
    var persistence = mutationOptions.Persistence ??
                      throw new KafdeckConfigurationException(
                          "Mutation persistence must be configured when mutation mode is enabled.");

    builder.Services.AddSingleton<IMutationMaterialDigestService>(services =>
        new HmacMutationMaterialDigestService(
            services.GetRequiredService<SecretResolver>()
                .Resolve(mutationOptions.MaterialDigestKey!)
                .Reveal()));

    builder.Services.AddSingleton<IMutationDbConnectionFactory>(services =>
        persistence.Provider switch
        {
            MutationPersistenceProvider.Sqlite =>
                new SqliteMutationDbConnectionFactory(persistence.SqliteDatabasePath!),
            MutationPersistenceProvider.PostgreSql =>
                new PostgreSqlMutationDbConnectionFactory(
                    services.GetRequiredService<SecretResolver>()
                        .Resolve(persistence.ConnectionString!)
                        .Reveal()),
            _ => throw new KafdeckConfigurationException(
                "Unsupported mutation persistence provider."),
        });

    builder.Services.AddSingleton<IMutationOperationRepository>(services =>
        new AdoMutationOperationRepository(
            services.GetRequiredService<IMutationDbConnectionFactory>(),
            captureProductNotifications: productEventsEnabled,
            notificationProductPolicy: productEventsEnabled
                ? services.GetRequiredService<NotificationProductEventPolicy>() : null,
            governance: services.GetRequiredService<IGovernancePolicyTransactionGate>()));
    builder.Services.AddSingleton<IMutationOperationTransactionReader>(services =>
        new AdoMutationOperationRepository(services.GetRequiredService<IMutationDbConnectionFactory>()));
    builder.Services.AddSingleton<AdoGovernanceGenerationGate>(services => new(
        services.GetRequiredService<IMutationDbConnectionFactory>(), governanceOptions.DeploymentGeneration));
    builder.Services.AddSingleton<IGovernanceGenerationTransactionGate>(services => services.GetRequiredService<AdoGovernanceGenerationGate>());
    builder.Services.AddKafdeckCatalog(governanceOptions.DeploymentGeneration?.ToString("D") ?? "unconfigured");
    builder.Services.AddSingleton<IServiceRequestCurrentAuthority,ServiceRequestAuthority>();
    builder.Services.AddSingleton<AdoServiceRequestStore>();
    builder.Services.AddSingleton<IApprovedServiceRequestTransactionReader>(services=>services.GetRequiredService<AdoServiceRequestStore>());
    builder.Services.AddSingleton<AdoGovernancePolicyStore>(services => new(
        services.GetRequiredService<IMutationDbConnectionFactory>(),
        services.GetRequiredService<IPolicyMetadataTransactionReader>(),
        services.GetRequiredService<IGovernanceGenerationTransactionGate>(),
        services.GetRequiredService<IApprovedServiceRequestTransactionReader>()));
    builder.Services.AddSingleton<IGovernancePolicyStore>(services => services.GetRequiredService<AdoGovernancePolicyStore>());
    builder.Services.AddSingleton<IGovernancePolicyTransactionGate>(services => services.GetRequiredService<AdoGovernancePolicyStore>());
    builder.Services.AddKafdeckAttribution(attributionOptions,governanceOptions.AttributionEnabled);
    builder.Services.AddSingleton<GovernancePolicyRuntime>();
    builder.Services.AddSingleton<GovernancePolicyMutationService>();
    builder.Services.AddSingleton<ServiceRequestMutationService>();
    builder.Services.AddSingleton<IMutationExecutionHandler>(services=>new ServiceRequestExecutionHandler(
        MutationOperationKind.ServiceRequestChange,services.GetRequiredService<ServiceRequestMutationService>()));
    builder.Services.AddSingleton<IMutationExecutionHandler>(services=>new ServiceRequestExecutionHandler(
        MutationOperationKind.ServiceRequestDecision,services.GetRequiredService<ServiceRequestMutationService>()));
    builder.Services.AddSingleton<IMutationExecutionHandler>(services => new GovernancePolicyExecutionHandler(
        MutationOperationKind.GovernancePolicyVersion, services.GetRequiredService<GovernancePolicyMutationService>()));
    builder.Services.AddSingleton<IMutationExecutionHandler>(services => new GovernancePolicyExecutionHandler(
        MutationOperationKind.GovernancePolicyActivation, services.GetRequiredService<GovernancePolicyMutationService>()));
    builder.Services.AddSingleton<IFleetMutationStateStore>(services =>
        new AdoFleetMutationStateStore(
            services.GetRequiredService<IMutationDbConnectionFactory>(),
            transactionReader: services.GetRequiredService<IMutationOperationTransactionReader>(),
            policyGate: services.GetRequiredService<IGovernancePolicyTransactionGate>()));
    builder.Services.AddSingleton<IConnectAutoRestartStateStore>(services =>
        new AdoConnectAutoRestartStateStore(
            services.GetRequiredService<IMutationDbConnectionFactory>(),
            services.GetRequiredService<IGovernancePolicyTransactionGate>(),
            services.GetRequiredService<IMutationOperationTransactionReader>()));
    builder.Services.AddSingleton<IMutationAuditSink, LoggingMutationAuditSink>();
    builder.Services.AddSingleton(services => new MutationApprovalAuthorizer(
        services.GetRequiredService<AuthorizationPolicyEvaluator>(),
        managedIdentity is null ? null : (human, request) =>
            ManagedIdentityEndpoints.IsRegisteredTarget(managedIdentity, request.ClusterId)
                ? ManagedIdentityEndpoints.IsRegisteredMutationAction(request.Action) &&
                  ManagedIdentityEndpoints.CanManageRegisteredTarget(managedIdentity.Find(human), request.ClusterId)
                : null));
    builder.Services.AddSingleton<MutationRequestAuthorizationService>();
    builder.Services.AddSingleton<MutationAdmissionService>();
    builder.Services.AddSingleton<MutationCommandService>();
    builder.Services.AddSingleton<MutationExecutionRequestContextAccessor>();
    builder.Services.AddSingleton<MutationDispatchService>();

    builder.Services.AddSingleton<TopicMutationPlanner>();
    builder.Services.AddSingleton<TopicMutationPreconditionValidator>();
    builder.Services.AddSingleton<ITopicMutationPort>(services =>
        new ConfluentKafkaTopicMutationAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredClient: RegisteredKafkaClient(services)));
    builder.Services.AddSingleton<TopicMutationExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicCreateExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicIncreasePartitionsExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, TopicDeleteExecutionHandler>();

    builder.Services.AddSingleton<RecordProductionPlanner>();
    builder.Services.AddSingleton<RecordProductionPreconditionValidator>();
    builder.Services.AddSingleton<IRecordProduceMutationPort>(services =>
        new ConfluentKafkaRecordProduceAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredProducerConfig: services.GetService<RegisteredKafkaRuntime>() is { } runtime ? runtime.GetProducerConfig : null));
    builder.Services.AddSingleton<RecordProductionExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, RecordProduceExecutionHandler>();

    builder.Services.AddSingleton<ClusterTransferPlanner>();
    builder.Services.AddSingleton<ClusterTransferSourceReader>();
    builder.Services.AddSingleton<IClusterTransferProducePort>(_ =>
        new ConfluentKafkaClusterTransferProduceAdapter(
            kafdeckOptions.Clusters,
            secretResolver));
    builder.Services.AddSingleton<GovernedDataJobPlanner>();
    builder.Services.AddSingleton<GovernedDataJobPreconditionValidator>();
    builder.Services.AddSingleton<GovernedDataJobStateCoordinator>();
    builder.Services.AddSingleton<GovernedDataJobSourceReader>();
    builder.Services.AddSingleton<ConfiguredGovernedDataJobEffectGuard>();
    builder.Services.AddSingleton<IGovernedDataJobEffectGuard>(services =>
        services.GetRequiredService<ConfiguredGovernedDataJobEffectGuard>());
    builder.Services.AddSingleton<GovernedDataJobDispatchCoordinator>();
    builder.Services.AddSingleton(
        GovernedDataJobWorkerPolicy.Default);
    builder.Services.AddSingleton<GovernedDataJobWorker>();
    builder.Services.AddSingleton<IMutationExecutionHandler,
        GovernedDataJobActivationHandler>();

    builder.Services.AddSingleton(
        new DataGeneratorDeploymentPolicy(
            (kafdeckOptions.Generator?.EnabledClusterIds ??
             Array.Empty<string>())
            .ToHashSet(StringComparer.Ordinal)));
    builder.Services.AddSingleton<DataGeneratorPlanner>();
    builder.Services.AddSingleton<DataGeneratorPreconditionValidator>();
    builder.Services.AddSingleton<DataGeneratorStateCoordinator>();
    builder.Services.AddSingleton<DataGeneratorMaterializer>();
    builder.Services.AddSingleton<ConfiguredDataGeneratorEffectGuard>();
    builder.Services.AddSingleton<IDataGeneratorEffectGuard>(services =>
        services.GetRequiredService<ConfiguredDataGeneratorEffectGuard>());
    builder.Services.AddSingleton<DataGeneratorDispatchCoordinator>();
    builder.Services.AddSingleton(
        DataGeneratorWorkerPolicy.Default);
    builder.Services.AddSingleton<DataGeneratorWorker>();
    builder.Services.AddSingleton<IMutationExecutionHandler,
        DataGeneratorActivationHandler>();

    builder.Services.AddSingleton<IConsumerMutationObservationPort>(services =>
        new ConfluentKafkaConsumerMutationObservationAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredClient: RegisteredKafkaClient(services)));
    builder.Services.AddSingleton<IConsumerMutationPort>(services =>
        new ConfluentKafkaConsumerMutationAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredClient: RegisteredKafkaClient(services)));
    builder.Services.AddSingleton<ConsumerMutationPlanner>();
    builder.Services.AddSingleton<ConsumerMutationPreconditionValidator>();
    builder.Services.AddSingleton<ConsumerMutationExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConsumerOffsetAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConsumerDeleteExecutionHandler>();

    builder.Services.AddSingleton<ConfluentSchemaMutationAdapter>(services =>
        new ConfluentSchemaMutationAdapter(
            kafdeckOptions.Clusters,
            secretResolver,
            registeredClient: services.GetService<RegisteredHttpServiceRuntime>() is { } registered ? registered.OpenRegistry : null));
    builder.Services.AddSingleton<ISchemaMutationPort>(services =>
        services.GetRequiredService<ConfluentSchemaMutationAdapter>());
    builder.Services.AddSingleton<ISchemaMutationObservationPort>(services =>
        services.GetRequiredService<ConfluentSchemaMutationAdapter>());
    builder.Services.AddSingleton(services => new SchemaMutationPlanner(
        services.GetRequiredService<ISchemaCatalogReadPort>(),
        services.GetRequiredService<ISchemaMutationObservationPort>(),
        services.GetRequiredService<IMutationMaterialDigestService>(),
        services.GetService<SchemaMutationPolicy>(), services.GetService<TimeProvider>(),
        registryRevision: services.GetService<RegisteredHttpServiceRuntime>() is { } registered ? registered.RegistryRevision : null));
    builder.Services.AddSingleton<SchemaMutationPreconditionValidator>();
    builder.Services.AddSingleton<SchemaMutationExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, SchemaCreateExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, SchemaAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, SchemaDeleteExecutionHandler>();

    builder.Services.AddSingleton<KafkaConnectMutationAdapter>(services =>
    {
        var registered = services.GetService<RegisteredHttpServiceRuntime>();
        return new KafkaConnectMutationAdapter(kafdeckOptions.Clusters, secretResolver,
            registeredClient: registered is null ? null : registered.OpenConnect,
            registeredRevision: registered is null ? null : registered.ConnectRevision);
    });
    builder.Services.AddSingleton<IConnectMutationPort>(services =>
        services.GetRequiredService<KafkaConnectMutationAdapter>());
    builder.Services.AddSingleton<IConnectMutationObservationPort>(services =>
        services.GetRequiredService<KafkaConnectMutationAdapter>());
    builder.Services.AddSingleton<ConnectMutationPlanner>();
    builder.Services.AddSingleton<ConnectMutationPreconditionValidator>();
    builder.Services.AddSingleton<ConnectMutationExecutionService>();
    builder.Services.AddSingleton<IConnectAutoRestartRuntimePolicyProvider,
        ConfiguredConnectAutoRestartRuntimePolicyProvider>();
    builder.Services.AddSingleton<IConnectAutoRestartGovernancePort,
        ConfiguredConnectAutoRestartGovernancePort>();
    builder.Services.AddSingleton<ConnectAutoRestartAttemptRevalidator>();
    builder.Services.AddSingleton<IConnectAutoRestartAttemptRevalidator>(services =>
        services.GetRequiredService<ConnectAutoRestartAttemptRevalidator>());
    builder.Services.AddSingleton<ConnectAutoRestartTypedDispatchAdapter>();
    builder.Services.AddSingleton<IConnectAutoRestartDispatchPort>(services =>
        services.GetRequiredService<ConnectAutoRestartTypedDispatchAdapter>());
    builder.Services.AddSingleton<IConnectAutoRestartAuditSink,
        LoggingConnectAutoRestartAuditSink>();
    builder.Services.AddSingleton<ConnectAutoRestartController>();
    builder.Services.AddSingleton<ConnectAutoRestartPolicyPlanner>();
    builder.Services.AddSingleton<ConnectAutoRestartPolicyPreconditionValidator>();
    builder.Services.AddSingleton<IMutationExecutionHandler,
        ConnectAutoRestartPolicyExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConnectCreateExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConnectAlterExecutionHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler, ConnectDeleteExecutionHandler>();

    builder.Services.AddSingleton<IRecordsPurgeObservationPort>(services =>
        new ConfluentKafkaRecordsPurgeObservationAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredClient: RegisteredKafkaClient(services)));
    builder.Services.AddSingleton<IRecordsPurgeMutationPort>(services =>
        new ConfluentKafkaRecordsPurgeAdapter(
            kafdeckOptions.Clusters,
            secretResolver, registeredClient: RegisteredKafkaClient(services)));
    builder.Services.AddSingleton<RecordsPurgePlanner>();
    builder.Services.AddSingleton<RecordsPurgePreconditionValidator>();
    builder.Services.AddSingleton<RecordsPurgeExecutionService>();
    builder.Services.AddSingleton<IMutationExecutionHandler, RecordsPurgeExecutionHandler>();

    builder.Services.AddSingleton<W39MutationPreDispatchGuard>();
    builder.Services.AddSingleton<IMutationPreDispatchGuard>(services => new GovernanceMutationPreDispatchGuard(
        services.GetRequiredService<W39MutationPreDispatchGuard>(), services.GetRequiredService<GovernancePolicyRuntime>(),
        services.GetRequiredService<GovernancePolicyMutationService>(),services.GetRequiredService<ServiceRequestMutationService>(),
        services.GetRequiredService<AttributionMutationService>()));
    builder.Services.AddSingleton<MutationExecutionHandlerRegistry>(services =>
        new MutationExecutionHandlerRegistry(
            services.GetServices<IMutationExecutionHandler>()));
    builder.Services.AddSingleton(
        new MutationExecutorPolicy(
            mutationOptions.MaxConcurrentPerCluster,
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMinutes(2)));
    builder.Services.AddSingleton<IDestinationDispatchObserver>(services => new ConfluentDestinationDispatchObserver(
        kafdeckOptions.Clusters, services.GetRequiredService<SecretResolver>()));
    builder.Services.AddSingleton<JobReconciliationService>();
    builder.Services.AddSingleton<IMutationExecutionHandler>(services => services.GetRequiredService<JobReconciliationService>());
    builder.Services.AddSingleton<MutationExecutor>();
    builder.Services.AddSingleton<MutationRecoveryCoordinator>();
    builder.Services.AddHostedService<MutationRecoveryHostedService>();
    if (builder.Configuration.GetValue("Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled", true))
    {
        builder.Services.AddHostedService<GovernedDataJobHostedService>();
        builder.Services.AddHostedService<DataGeneratorHostedService>();
    }
    builder.Services.AddHostedService<ConnectAutoRestartHostedService>();
}

builder.Services.AddKafdeckNotificationProductEvents(productEventsEnabled);
// Keep durable policy authority reachable when mutation endpoints/workers are disabled.
if (mutationOptions is { Enabled: false, Persistence: { } dormantPersistence })
{
    builder.Services.AddSingleton<IMutationDbConnectionFactory>(services => dormantPersistence.Provider switch
    {
        MutationPersistenceProvider.Sqlite => new SqliteMutationDbConnectionFactory(dormantPersistence.SqliteDatabasePath!),
        MutationPersistenceProvider.PostgreSql => new PostgreSqlMutationDbConnectionFactory(services.GetRequiredService<SecretResolver>().Resolve(dormantPersistence.ConnectionString!).Reveal()),
        _ => throw new KafdeckConfigurationException("Unsupported governance authority persistence provider."),
    });
    builder.Services.AddSingleton<AdoGovernanceGenerationGate>(services => new(services.GetRequiredService<IMutationDbConnectionFactory>(), governanceOptions.DeploymentGeneration));
    builder.Services.AddSingleton<IGovernanceGenerationTransactionGate>(services => services.GetRequiredService<AdoGovernanceGenerationGate>());
    builder.Services.AddSingleton<AdoCatalogStore>(services => new(services.GetRequiredService<IMutationDbConnectionFactory>(),
        governanceOptions.DeploymentGeneration?.ToString("D") ?? "unconfigured", services.GetRequiredService<IGovernanceGenerationTransactionGate>()));
    builder.Services.AddSingleton<ICatalogStore>(services => services.GetRequiredService<AdoCatalogStore>());
    builder.Services.AddSingleton<IPolicyMetadataTransactionReader>(services => services.GetRequiredService<AdoCatalogStore>());
    builder.Services.AddSingleton<IMutationOperationTransactionReader>(services=>new AdoMutationOperationRepository(services.GetRequiredService<IMutationDbConnectionFactory>()));
    builder.Services.AddSingleton<IServiceRequestCurrentAuthority,ServiceRequestAuthority>();
    builder.Services.AddSingleton<AdoServiceRequestStore>();
    builder.Services.AddSingleton<IApprovedServiceRequestTransactionReader>(services=>services.GetRequiredService<AdoServiceRequestStore>());
    builder.Services.AddSingleton<AdoGovernancePolicyStore>(services => new(services.GetRequiredService<IMutationDbConnectionFactory>(),
        services.GetRequiredService<IPolicyMetadataTransactionReader>(), services.GetRequiredService<IGovernanceGenerationTransactionGate>(),
        services.GetRequiredService<IApprovedServiceRequestTransactionReader>()));
    builder.Services.AddSingleton<IGovernancePolicyStore>(services => services.GetRequiredService<AdoGovernancePolicyStore>());
    builder.Services.AddSingleton<IGovernancePolicyTransactionGate>(services => services.GetRequiredService<AdoGovernancePolicyStore>());
    builder.Services.AddSingleton<AdoAttributionStore>(services=>new(services.GetRequiredService<IMutationDbConnectionFactory>(),
        services.GetRequiredService<IGovernanceGenerationTransactionGate>(),services.GetRequiredService<IGovernancePolicyTransactionGate>(),
        services.GetRequiredService<AdoCatalogStore>()));
    builder.Services.AddSingleton<IAttributionStore>(services=>services.GetRequiredService<AdoAttributionStore>());
}
if (notificationGovernance is not null) builder.Services.AddKafdeckNotificationGovernance(notificationGovernance);
if (notificationRecovery is not null)
{
    var recoverySource = mutationOptions?.Persistence ?? throw new KafdeckConfigurationException(
        "Notification recovery requires an explicit server-owned mutation source inventory.");
    builder.Services.AddSingleton<INotificationRecoverySourceInventory>(services => recoverySource.Provider switch
    {
        MutationPersistenceProvider.Sqlite => AdoNotificationRecoverySourceInventory.ForSqlite(recoverySource.SqliteDatabasePath!),
        MutationPersistenceProvider.PostgreSql => AdoNotificationRecoverySourceInventory.ForPostgreSql(
            services.GetRequiredService<SecretResolver>().Resolve(recoverySource.ConnectionString!).Reveal()),
        _ => throw new KafdeckConfigurationException("Notification recovery source provider is unsupported."),
    });
    builder.Services.AddKafdeckNotificationRecovery(notificationRecovery);
}

if (declarativeOptions.Enabled)
{
    builder.Services.AddSingleton<IW68RuntimeStore>(services => new W68RuntimeStore(
        services.GetRequiredService<INotificationDeliveryDbConnectionFactory>(), declarativeOptions.DeploymentGeneration));
    builder.Services.AddSingleton<DeclarativeRuntimeAuthorizer>();
    builder.Services.AddSingleton<DeclarativeLifecycleGuard>();
    builder.Services.AddSingleton<DeclarativeEndpointService>();
    builder.Services.AddSingleton<DeclarativeSubscriptionMutationHandler>();
    builder.Services.AddSingleton<IMutationExecutionHandler>(services => services.GetRequiredService<DeclarativeSubscriptionMutationHandler>());
}
var app = builder.Build();
if (governanceGenerationAdmission)
{
    var previous = Guid.ParseExact(builder.Configuration["Kafdeck:Governance:PreviousDeploymentGeneration"] ?? "", "D");
    await app.Services.GetRequiredService<AdoGovernanceGenerationGate>().AdmitRestoredGenerationAsync(previous,
        governanceOptions.DeploymentGeneration!.Value,
        builder.Configuration["Kafdeck:Governance:FencedWorkersEvidenceSha256"] ?? "",
        builder.Configuration["Kafdeck:Governance:RestoreEvidenceSha256"] ?? "");
    app.Logger.LogInformation("Governance restore generation admitted in quarantine; HTTP and workers were not started.");
    await app.DisposeAsync();
    return;
}
if (declarativeGenerationAdmission)
{
    if (!DeclarativeRecoveryAnchor.Allows(declarativeOptions, maintenance: true))
        throw new KafdeckConfigurationException("Generation admission requires the pinned quarantined outside authority.");
    var previous = builder.Configuration["Kafdeck:Declarative:PreviousDeploymentGeneration"];
    Guid? previousGeneration = string.IsNullOrEmpty(previous) ? null : Guid.ParseExact(previous, "D");
    await W68RuntimeSchema.AdmitGenerationAsync(app.Services.GetRequiredService<INotificationDeliveryDbConnectionFactory>(),
        declarativeOptions.DeploymentId, previousGeneration, declarativeOptions.DeploymentGeneration,
        builder.Configuration["Kafdeck:Declarative:GenerationFenceEvidenceSha256"] ?? "");
    app.Logger.LogInformation("Declarative generation admission verified; outside authority remains quarantined and no workers started.");
    await app.DisposeAsync();
    return;
}
if (declarativeSchemaMigration)
{
    await W68RuntimeSchema.InstallOrUpgradeAsync(app.Services.GetRequiredService<INotificationDeliveryDbConnectionFactory>(),
        declarativeSchemaAdoption, builder.Configuration["Kafdeck:Declarative:AdoptionEvidenceSha256"]);
    app.Logger.LogInformation("Declarative schema migration verified; HTTP server and workers were not started.");
    await app.DisposeAsync();
    return;
}
if (notificationGovernance is not null) await app.Services.InitializeKafdeckNotificationGovernanceAsync();
if (mutationOptions?.Persistence is not null)
{
    await app.Services.GetRequiredService<AdoGovernanceGenerationGate>().InitializeAsync();
    await app.Services.GetRequiredService<ICatalogStore>().InitializeAsync(CancellationToken.None);
    await app.Services.GetRequiredService<AdoServiceRequestStore>().InitializeAsync();
    await app.Services.GetRequiredService<AdoGovernancePolicyStore>().InitializeAsync();
    await app.Services.GetRequiredService<AdoAttributionStore>().InitializeAsync();
}

if (historicalMetricsOptions?.Enabled == true)
{
    var historicalMetricStore =
        app.Services.GetRequiredService<
            IHistoricalMetricStore>();
    await historicalMetricStore
        .InitializeAsync()
        .ConfigureAwait(false);

    var historicalMetricMaintenanceStore =
        app.Services.GetRequiredService<
            IHistoricalMetricMaintenanceStore>();
    await historicalMetricMaintenanceStore
        .InitializeAsync()
        .ConfigureAwait(false);

    app.Logger.LogInformation(
        "Kafdeck historical metrics provider initialized with provider {Provider}, execution mode {ExecutionMode}, raw retention {RawRetentionHours}h and rollup retention {RollupRetentionDays}d.",
        historicalMetricsOptions.Provider,
        historicalMetricsOptions.ExecutionMode,
        historicalMetricsOptions.RawRetentionHours,
        historicalMetricsOptions.RollupRetentionDays);
}

if (dataQualityOptions?.Enabled == true)
{
    var dataQualityStore =
        app.Services.GetRequiredService<
            IDataQualityLifecycleStore>();
    await dataQualityStore
        .InitializeAsync()
        .ConfigureAwait(false);

    app.Logger.LogInformation(
        "Kafdeck data-quality persistence initialized with provider {Provider}, execution mode {ExecutionMode}, management enabled {ManagementEnabled}.",
        dataQualityOptions.Provider,
        dataQualityOptions.ExecutionMode,
        dataQualityOptions.ManagementEnabled);
}

if (notificationReadOptions?.Enabled == true)
{
    // Start-up fails closed on incompatible notification schema,
    // including an unresolved v1 delivery backlog, before hosted workers start.
    await app.Services.GetRequiredService<INotificationDeliveryStore>()
        .InitializeAsync()
        .ConfigureAwait(false);
    await app.Services.GetRequiredService<INotificationRoutingStore>()
        .InitializeAsync()
        .ConfigureAwait(false);
    if (declarativeOptions.Enabled)
        await app.Services.GetRequiredService<IW68RuntimeStore>().InitializeAsync().ConfigureAwait(false);
    if (notificationRecovery is not null) await app.Services.InitializeKafdeckNotificationRecoveryAsync();

    app.Logger.LogInformation(
        "Kafdeck notification persistence initialized with provider {Provider}, mode {Mode}; subscription management API enabled {ManagementEnabled}; physical Webhook delivery enabled {WebhookDeliveryEnabled}; typed HTTP provider delivery enabled {HttpProviderDeliveryEnabled}; Email delivery enabled {EmailDeliveryEnabled}; verified product event capture enabled {ProductEventsEnabled}.",
        notificationReadOptions.Provider,
        notificationReadOptions.ExecutionMode,
        notificationReadOptions.ManagementEnabled,
        notificationReadOptions.WebhookDeliveryEnabled,
        notificationReadOptions.HttpProviderDeliveryEnabled,
        notificationReadOptions.EmailDeliveryEnabled,
        productEventsEnabled);
}

app.Logger.LogInformation(
    "Kafdeck startup configuration: {@Configuration}",
    SafeConfigurationDiagnostics.Create(kafdeckOptions));

if (mutationOptions?.Enabled == true)
{
    _ = app.Services.GetRequiredService<IMutationMaterialDigestService>();
    var mutationRepository = app.Services.GetRequiredService<IMutationOperationRepository>();
    await mutationRepository.InitializeAsync().ConfigureAwait(false);

    var fleetStateStore =
        app.Services.GetRequiredService<IFleetMutationStateStore>();
    await fleetStateStore.InitializeAsync().ConfigureAwait(false);

    var autoRestartStore =
        app.Services.GetRequiredService<IConnectAutoRestartStateStore>();
    await autoRestartStore.InitializeAsync().ConfigureAwait(false);

    var mutationRecovery = app.Services.GetRequiredService<MutationRecoveryCoordinator>();
    var recoveredMutations = await mutationRecovery
        .RecoverInterruptedExecutionsAsync(
            ignoreActiveExecutionLeases:
                mutationOptions.Persistence!.ExecutionMode == MutationExecutionMode.Standalone)
        .ConfigureAwait(false);

    app.Logger.LogInformation(
        "Kafdeck mutation runtime reconciled {RecoveredMutationCount} interrupted execution(s).",
        recoveredMutations);

    app.Logger.LogInformation(
        "Kafdeck mutation runtime initialized with governed W39 topic, record-production, consumer, schema, Connect and controlled-purge dispatch, persistence provider {PersistenceProvider} and execution mode {ExecutionMode}.",
        mutationOptions.Persistence!.Provider,
        mutationOptions.Persistence.ExecutionMode);
}

foreach (var cluster in kafdeckOptions.Clusters.Where(cluster =>
             cluster.SecurityProtocol is KafkaSecurityProtocol.Plaintext or KafkaSecurityProtocol.SaslPlaintext))
{
    app.Logger.LogWarning(
        "Cluster profile {ClusterId} uses insecure Kafka transport protocol {SecurityProtocol}.",
        cluster.Id,
        cluster.SecurityProtocol);
}

app.UseExceptionHandler();
app.UseMiddleware<ApiTelemetryMiddleware>();

if (prometheusScrapeToken is not null)
{
    app.UseWhen(
        context =>
            KafdeckObservabilityEndpoints
                .IsPrometheusScrapePath(
                    context.Request.Path),
        branch => branch.UseMiddleware<PrometheusScrapeTokenMiddleware>(
            prometheusScrapeToken));
}

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    app.UseAuthentication();
    app.UseAntiforgery();
}

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseMiddleware<NonBrowserReadBoundaryMiddleware>();

static bool IsBrowserAccessModeRequest(HttpContext context) =>
    HttpMethods.IsGet(context.Request.Method) &&
    context.Request.Path.Equals(new PathString("/api/v1/auth/mode"), StringComparison.OrdinalIgnoreCase);

bool IsSetupRequest(HttpContext context) => managedIdentity is not null &&
    (HttpMethods.IsGet(context.Request.Method) || HttpMethods.IsPost(context.Request.Method)) &&
    context.Request.Path.Equals(new PathString("/api/v1/setup"), StringComparison.OrdinalIgnoreCase);

if (deploymentAccessToken is not null)
{
    app.UseWhen(
        context => context.Request.Path.StartsWithSegments("/api") &&
            !IsBrowserAccessModeRequest(context) && context.User.Identity is not NonBrowserReadIdentity,
        branch => branch.UseMiddleware<DeploymentAccessTokenMiddleware>(deploymentAccessToken));
}

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    app.UseWhen(
        context =>
            context.Request.Path.StartsWithSegments("/api/v1") &&
            !IsBrowserAccessModeRequest(context) &&
            !IsSetupRequest(context) &&
            !context.Request.Path.StartsWithSegments("/api/v1/auth/login"),
        branch => branch.UseMiddleware<OidcApiAuthenticationBoundaryMiddleware>());
}

app.MapGet("/healthz", () => Results.Ok(new
    {
        status = "ok",
        product = ProductIdentity.Name,
    }))
    .WithName("healthz");

var browserAccessMode = kafdeckOptions.Deployment.Mode.ToString().ToLowerInvariant();
var connectionWorkspace = kafdeckOptions.Deployment.Mode == AccessMode.Oidc &&
    app.Configuration.GetValue<bool>("Kafdeck:ConnectionWorkspace:Enabled");
app.MapGet("/api/v1/auth/mode", (HttpContext context) =>
{
    context.Response.Headers.CacheControl = "no-store";
    return Results.Ok(new
    {
        authenticationMode = browserAccessMode, connectionWorkspace,
        setupState = managedIdentity is null ? null : ManagedIdentityEndpoints.SetupState(managedIdentity.Read()),
    });
});

if (kafdeckOptions.Deployment.Mode == AccessMode.Oidc)
{
    app.MapKafdeckOidcSessionEndpoints(connectionWorkspace);
    app.MapKafdeckAntiforgeryEndpoint();
    if (connectionWorkspace) app.MapKafkaConnectionWorkspace(kafdeckOptions, managedIdentity);
    if (managedIdentity is not null)
    {
        _ = app.Services.GetRequiredService<ManagedKeycloakUsers>();
        app.MapManagedIdentity(managedIdentity, kafdeckOptions);
        if (app.Services.GetService<RegisteredHttpServiceRuntime>() is { } registered)
            app.MapRegisteredHttpServices(managedIdentity, registered);
    }
}
else
{
    app.MapGet("/api/v1/auth/session", (HttpContext context) =>
    {
        context.Response.Headers.CacheControl = "no-store";
        if (context.User.Identity is NonBrowserReadIdentity)
        {
            return Results.Unauthorized();
        }

        return Results.Ok(new { authenticated = true, authenticationMode = browserAccessMode });
    });
}

var dashboardOptions = app.Services.GetService<RegisteredKafkaRuntime>() is { } registeredKafka
    ? kafdeckOptions with { Clusters = registeredKafka.IncludeRegistered(kafdeckOptions.Clusters) }
    : kafdeckOptions;
app.MapKafdeckV01(dashboardOptions);
app.MapKafdeckRecordEndpoints(kafdeckOptions);
app.MapKafdeckV04ReadViews(dashboardOptions);
app.MapKafdeckV07SchemaCapabilities(dashboardOptions);
app.MapKafdeckV07SchemaDeveloperTools(dashboardOptions);
app.MapKafdeckV07Streaming(kafdeckOptions);
app.MapKafdeckV07ControlledSerde(kafdeckOptions);
app.MapKafdeckV08Observability(kafdeckOptions);
app.MapKafdeckV08OperationalAnalytics(kafdeckOptions);
if (dataQualityOptions?.Enabled == true)
{
    app.MapKafdeckV08DataQuality(kafdeckOptions);
}
if (notificationReadOptions?.Enabled == true)
{
    app.MapKafdeckV08NotificationReads();
    if (productEventsEnabled) app.MapKafdeckNotificationProductEventStatus();
    if (notificationReadOptions.ManagementEnabled)
    {
        app.MapKafdeckV08NotificationManagement();
        if (notificationGovernance is not null) app.MapKafdeckNotificationGovernance();
        if (notificationRecovery is not null) app.MapKafdeckNotificationRecovery();
    }
}
app.MapKafdeckFleetCapabilities();
app.MapKafdeckV06OpenApi();
app.MapKafdeckV07OpenApi();
app.MapKafdeckV08BearerOpenApi();
app.MapKafdeckBearerCapabilities(mutationOptions?.Enabled == true, declarativeEnabled: declarativeOptions.Enabled);
app.MapKafdeckDeclarative(declarativeOptions.Enabled);
if (governanceOptions.CatalogEnabled) app.MapKafdeckCatalog();
if (governanceOptions.PoliciesEnabled) app.MapKafdeckGovernancePolicies();
if (governanceOptions.RequestsEnabled) app.MapKafdeckServiceRequestEndpoints();
if (governanceOptions.AttributionEnabled) app.MapKafdeckAttribution();
app.MapGovernanceOpenApi();
if (mutationOptions?.Enabled == true)
{
    app.MapKafdeckMutationEndpoints();
    app.MapKafdeckTopicMutationEndpoints();
    app.MapKafdeckBearerMutations();
    app.MapKafdeckRecordProductionEndpoints();
    app.MapKafdeckDataJobEndpoints();
    app.MapKafdeckDataGeneratorEndpoints();
    app.MapKafdeckConsumerMutationEndpoints();
    app.MapKafdeckSchemaMutationEndpoints();
    app.MapKafdeckConnectMutationEndpoints();
    app.MapKafdeckConnectAutoRestartEndpoints();
    app.MapKafdeckRecordsPurgeEndpoints();
}
app.MapFallbackToFile("index.html");

app.Run();

public partial class Program;
