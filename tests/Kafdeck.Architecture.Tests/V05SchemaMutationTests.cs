using System.Text.Json;
using Kafdeck.Core.ReadViews;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Core.Schemas;
using Kafdeck.Modules.Administration;
using Kafdeck.Modules.Schemas;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V05SchemaMutationTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 22, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Create_planner_binds_compatibility_references_and_keeps_schema_ephemeral()
    {
        const string secretSchema =
            """{"type":"record","name":"SensitiveOrder","fields":[{"name":"secret_field","type":"string"}]}""";

        var catalog = new FakeCatalog
        {
            Versions =
            [
                new SchemaVersionSummary(
                    "orders-value",
                    2,
                    20,
                    RecordSchemaFormat.Avro,
                    Array.Empty<RecordSchemaReference>()),
            ],
            Compatibility =
                new SchemaCompatibilityObservation(
                    "orders-value",
                    SchemaCompatibilityMode.Backward,
                    false),
            ReferenceDetail =
                Detail(
                    "common-value",
                    1,
                    10,
                    """{"type":"record","name":"Common","fields":[]}"""),
        };
        var observation = new FakeMutationObservation();
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");

        var planner = new SchemaMutationPlanner(
            catalog,
            observation,
            digest,
            timeProvider: new FixedTimeProvider(Now));

        var result = await planner.PlanCreateAsync(
            new SchemaRegistrationRequest(
                "prod",
                "orders-value",
                RecordSchemaFormat.Avro,
                secretSchema,
                new[]
                {
                    new RecordSchemaReference(
                        "common.avsc",
                        "common-value",
                        1),
                }));

        Assert.True(result.IsSuccess, result.Failure?.SafeMessage);
        var plan = result.Plan!;
        Assert.Equal(MutationRiskClass.Moderate, plan.Risk.RiskClass);
        Assert.Equal(MutationConfirmationMode.Explicit, plan.Risk.ConfirmationMode);
        Assert.False(plan.Risk.RequiresIndependentApproval);
        Assert.True(plan.Canonical.CompatibilityValidated);
        Assert.Equal("schema_compatible", plan.Canonical.CompatibilityResultCode);
        Assert.Single(plan.Canonical.References);
        Assert.Equal(64, plan.Canonical.SchemaSha256.Length);
        Assert.DoesNotContain(
            "SensitiveOrder",
            plan.Intent.CanonicalIntent,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "secret_field",
            plan.Intent.CanonicalIntent,
            StringComparison.Ordinal);

        Assert.NotNull(plan.Intent.AuthorizationTargets);
        var target = Assert.Single(plan.Intent.AuthorizationTargets!);
        Assert.Equal(AuthorizationAction.SchemaCreate, target.Action);
        Assert.Equal("schema/orders-value", target.ResourceName);

        Assert.NotNull(plan.Intent.MaterialDigests);
        Assert.Single(plan.Intent.MaterialDigests!);
        Assert.Equal("schema/source", plan.Intent.MaterialDigests![0].Name);

        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            plan.Intent,
            plan.Risk,
            "w36-test",
            Now.AddMinutes(5),
            Now,
            "schema-create");

        var durable = JsonSerializer.Serialize(operation.Snapshot);
        Assert.DoesNotContain("SensitiveOrder", durable, StringComparison.Ordinal);
        Assert.DoesNotContain("secret_field", durable, StringComparison.Ordinal);

        Assert.NotNull(result.ExecutionMaterial);
        using var material = result.ExecutionMaterial!;
        var raw = material.GetRequired("schema/source");
        Assert.Equal(secretSchema, SchemaMutationCanonicalization.DecodeSchema(raw));
    }

    [Fact]
    public async Task Incompatible_schema_fails_before_execution_material_is_created()
    {
        var catalog = ExistingSubjectCatalog();
        var observation = new FakeMutationObservation
        {
            CompatibilityCheck =
                new SchemaCompatibilityCheckObservation(
                    false,
                    "schema_incompatible"),
        };
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");

        var result = await new SchemaMutationPlanner(
                catalog,
                observation,
                digest)
            .PlanCreateAsync(
                new SchemaRegistrationRequest(
                    "prod",
                    "orders-value",
                    RecordSchemaFormat.JsonSchema,
                    """{"type":"object"}""",
                    Array.Empty<RecordSchemaReference>()));

        Assert.False(result.IsSuccess);
        Assert.Equal(
            SchemaMutationPlanningFailureCode.SchemaIncompatible,
            result.Failure!.Code);
        Assert.Null(result.ExecutionMaterial);
    }

    [Fact]
    public async Task Compatibility_and_delete_risks_follow_governed_floors()
    {
        var catalog = ExistingSubjectCatalog();
        var observation = new FakeMutationObservation();
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new SchemaMutationPlanner(
            catalog,
            observation,
            digest);

        var compatibility = await planner.PlanCompatibilityAsync(
            new SchemaCompatibilityAlterRequest(
                "prod",
                SchemaCompatibilityScope.Subject,
                "orders-value",
                SchemaCompatibilityMode.Full));

        Assert.True(compatibility.IsSuccess, compatibility.Failure?.SafeMessage);
        Assert.Equal(MutationRiskClass.High, compatibility.Plan!.Risk.RiskClass);
        Assert.Equal(
            MutationConfirmationMode.TypedTarget,
            compatibility.Plan.Risk.ConfirmationMode);

        observation.DeleteObservation =
            new SchemaDeleteTargetObservation(
                true,
                true,
                false,
                new[] { 1, 2 },
                new[] { 1, 2 });

        var soft = await planner.PlanDeleteAsync(
            new SchemaDeleteRequest(
                "prod",
                "orders-value",
                version: null,
                Permanent: false));

        Assert.True(soft.IsSuccess, soft.Failure?.SafeMessage);
        Assert.Equal(MutationRiskClass.High, soft.Plan!.Risk.RiskClass);
        Assert.False(soft.Plan.Risk.RequiresIndependentApproval);

        observation.DeleteObservation =
            new SchemaDeleteTargetObservation(
                false,
                true,
                true,
                Array.Empty<int>(),
                new[] { 1, 2 });

        var permanent = await planner.PlanDeleteAsync(
            new SchemaDeleteRequest(
                "prod",
                "orders-value",
                version: null,
                Permanent: true));

        Assert.True(permanent.IsSuccess, permanent.Failure?.SafeMessage);
        Assert.Equal(MutationRiskClass.Critical, permanent.Plan!.Risk.RiskClass);
        Assert.True(permanent.Plan.Risk.RequiresIndependentApproval);

        observation.DeleteObservation =
            new SchemaDeleteTargetObservation(
                true,
                true,
                false,
                new[] { 1, 2 },
                new[] { 1, 2 });

        var unsafePermanent = await planner.PlanDeleteAsync(
            new SchemaDeleteRequest(
                "prod",
                "orders-value",
                version: null,
                Permanent: true));

        Assert.False(unsafePermanent.IsSuccess);
        Assert.Equal(
            SchemaMutationPlanningFailureCode.PermanentDeleteRequiresSoftDelete,
            unsafePermanent.Failure!.Code);
    }

    [Fact]
    public async Task Pre_dispatch_detects_subject_drift()
    {
        var catalog = ExistingSubjectCatalog();
        var observation = new FakeMutationObservation();
        using var digest = new HmacMutationMaterialDigestService(
            "0123456789abcdef0123456789abcdef");
        var planner = new SchemaMutationPlanner(
            catalog,
            observation,
            digest,
            timeProvider: new FixedTimeProvider(Now));

        var planned = await planner.PlanCreateAsync(
            new SchemaRegistrationRequest(
                "prod",
                "orders-value",
                RecordSchemaFormat.JsonSchema,
                """{"type":"object","title":"A"}""",
                Array.Empty<RecordSchemaReference>()));

        Assert.True(planned.IsSuccess, planned.Failure?.SafeMessage);
        using var material = planned.ExecutionMaterial!;
        var operation = MutationOperation.CreatePreview(
            "oidc:https://idp.example|alice",
            planned.Plan!.Intent,
            planned.Plan.Risk,
            "w36-stale",
            Now.AddMinutes(5),
            Now,
            "schema-stale");

        catalog.Versions =
        [
            new SchemaVersionSummary(
                "orders-value",
                1,
                10,
                RecordSchemaFormat.JsonSchema,
                Array.Empty<RecordSchemaReference>()),
            new SchemaVersionSummary(
                "orders-value",
                2,
                11,
                RecordSchemaFormat.JsonSchema,
                Array.Empty<RecordSchemaReference>()),
        ];

        var guard = new SchemaMutationPreconditionValidator(
            planner,
            catalog,
            observation,
            timeProvider: new FixedTimeProvider(Now));

        var result = await guard.ValidateAsync(operation.Snapshot);

        Assert.Equal(
            MutationPreDispatchGuardOutcome.StalePreview,
            result.Outcome);
    }

    [Fact]
    public void Schema_mutation_surface_has_no_generic_registry_write_proxy()
    {
        var methods = typeof(ISchemaMutationPort)
            .GetMethods()
            .OrderBy(method => method.Name, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(3, methods.Length);
        Assert.Equal(
            new[]
            {
                nameof(ISchemaMutationPort.AlterCompatibilityAsync),
                nameof(ISchemaMutationPort.CreateAsync),
                nameof(ISchemaMutationPort.DeleteAsync),
            },
            methods.Select(method => method.Name));

        var forbidden = new[]
        {
            "url",
            "path",
            "method",
            "header",
            "http",
            "proxy",
            "command",
        };

        foreach (var type in new[]
                 {
                     typeof(SchemaCreateMutation),
                     typeof(SchemaAlterMutation),
                     typeof(SchemaDeleteMutation),
                     typeof(SchemaMutationReference),
                 })
        {
            foreach (var property in type.GetProperties())
            {
                Assert.DoesNotContain(
                    forbidden,
                    term => property.Name.Contains(
                        term,
                        StringComparison.OrdinalIgnoreCase));
            }
        }
    }

    private static FakeCatalog ExistingSubjectCatalog() =>
        new()
        {
            Versions =
            [
                new SchemaVersionSummary(
                    "orders-value",
                    1,
                    10,
                    RecordSchemaFormat.JsonSchema,
                    Array.Empty<RecordSchemaReference>()),
            ],
            Compatibility =
                new SchemaCompatibilityObservation(
                    "orders-value",
                    SchemaCompatibilityMode.Backward,
                    false),
        };

    private static SchemaVersionDetail Detail(
        string subject,
        int version,
        int id,
        string schema) =>
        new(
            subject,
            version,
            new RecordSchemaDocument(
                id,
                RecordSchemaFormat.Avro,
                schema,
                Array.Empty<RecordSchemaReference>()));

    private sealed class FakeCatalog : ISchemaCatalogReadPort
    {
        public IReadOnlyList<SchemaVersionSummary> Versions { get; set; } =
            Array.Empty<SchemaVersionSummary>();

        public SchemaCompatibilityObservation Compatibility { get; set; } =
            new(
                "orders-value",
                SchemaCompatibilityMode.Backward,
                false);

        public SchemaGlobalCompatibilityObservation GlobalCompatibility { get; set; } =
            new(SchemaCompatibilityMode.Backward);

        public SchemaVersionDetail? ReferenceDetail { get; set; }

        public Task<ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>> ListSubjectsAsync(
            string clusterId,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<IReadOnlyList<SchemaSubjectSummary>>.Success(
                    Versions.Count > 0
                        ? new[] { new SchemaSubjectSummary("orders-value") }
                        : Array.Empty<SchemaSubjectSummary>()));

        public Task<ReadViewResult<IReadOnlyList<SchemaVersionSummary>>> ListVersionsAsync(
            string clusterId,
            string subject,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
        {
            if (string.Equals(subject, "orders-value", StringComparison.Ordinal))
            {
                return Task.FromResult(
                    ReadViewResult<IReadOnlyList<SchemaVersionSummary>>.Success(
                        Versions));
            }

            if (ReferenceDetail is not null &&
                string.Equals(subject, ReferenceDetail.Subject, StringComparison.Ordinal))
            {
                return Task.FromResult(
                    ReadViewResult<IReadOnlyList<SchemaVersionSummary>>.Success(
                        new[]
                        {
                            new SchemaVersionSummary(
                                ReferenceDetail.Subject,
                                ReferenceDetail.Version,
                                ReferenceDetail.Schema.Id,
                                ReferenceDetail.Schema.Format,
                                ReferenceDetail.Schema.References),
                        }));
            }

            return Task.FromResult(
                ReadViewResult<IReadOnlyList<SchemaVersionSummary>>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Unavailable,
                        "schema_registry_resource_not_found",
                        "not found",
                        false)));
        }

        public Task<ReadViewResult<SchemaVersionDetail>> GetVersionAsync(
            string clusterId,
            string subject,
            int version,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken)
        {
            if (ReferenceDetail is not null &&
                string.Equals(subject, ReferenceDetail.Subject, StringComparison.Ordinal) &&
                version == ReferenceDetail.Version)
            {
                return Task.FromResult(
                    ReadViewResult<SchemaVersionDetail>.Success(
                        ReferenceDetail));
            }

            var summary = Versions.SingleOrDefault(item =>
                item.Version == version &&
                string.Equals(item.Subject, subject, StringComparison.Ordinal));
            if (summary is not null)
            {
                return Task.FromResult(
                    ReadViewResult<SchemaVersionDetail>.Success(
                        new SchemaVersionDetail(
                            subject,
                            version,
                            new RecordSchemaDocument(
                                summary.SchemaId,
                                summary.Format,
                                """{"type":"object"}""",
                                summary.References))));
            }

            return Task.FromResult(
                ReadViewResult<SchemaVersionDetail>.Failed(
                    new ReadViewFailure(
                        ReadViewFailureCategory.Unavailable,
                        "schema_registry_resource_not_found",
                        "not found",
                        false)));
        }

        public Task<ReadViewResult<SchemaCompatibilityObservation>> GetCompatibilityAsync(
            string clusterId,
            string subject,
            ReadViewOperationContext operation,
            CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<SchemaCompatibilityObservation>.Success(
                    Compatibility with { Subject = subject }));

        public Task<ReadViewResult<SchemaGlobalCompatibilityObservation>>
            GetGlobalCompatibilityAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                ReadViewResult<SchemaGlobalCompatibilityObservation>.Success(
                    GlobalCompatibility));
    }

    private sealed class FakeMutationObservation :
        ISchemaMutationObservationPort
    {
        public SchemaMutationCapabilities Capabilities { get; set; } =
            new(
                true,
                true,
                true,
                true,
                true);

        public SchemaCompatibilityCheckObservation CompatibilityCheck { get; set; } =
            new(
                true,
                "schema_compatible");

        public SchemaDeleteTargetObservation DeleteObservation { get; set; } =
            new(
                true,
                true,
                false,
                new[] { 1 },
                new[] { 1 });

        public Task<SchemaMutationObservationResult<SchemaMutationCapabilities>>
            GetCapabilitiesAsync(
                string clusterId,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                SchemaMutationObservationResult<SchemaMutationCapabilities>.Success(
                    Capabilities));

        public Task<
            SchemaMutationObservationResult<SchemaCompatibilityCheckObservation>>
            TestCompatibilityAsync(
                string clusterId,
                SchemaCompatibilityCheckRequest request,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                SchemaMutationObservationResult<SchemaCompatibilityCheckObservation>.Success(
                    CompatibilityCheck));

        public Task<
            SchemaMutationObservationResult<SchemaDeleteTargetObservation>>
            ObserveDeleteTargetAsync(
                string clusterId,
                SchemaDeleteObservationRequest request,
                ReadViewOperationContext operation,
                CancellationToken cancellationToken) =>
            Task.FromResult(
                SchemaMutationObservationResult<SchemaDeleteTargetObservation>.Success(
                    DeleteObservation));
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
