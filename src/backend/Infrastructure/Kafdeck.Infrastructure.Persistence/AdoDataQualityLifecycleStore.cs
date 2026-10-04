using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Records;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoDataQualityLifecycleStore :
    IDataQualityLifecycleStore
{
    private const int SchemaVersion = 1;
    private const string Component = "data-quality";
    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly IDataQualityDbConnectionFactory
        _connectionFactory;

    public AdoDataQualityLifecycleStore(
        IDataQualityDbConnectionFactory connectionFactory)
    {
        _connectionFactory =
            connectionFactory ??
            throw new ArgumentNullException(
                nameof(connectionFactory));
    }

    public async Task InitializeAsync(
        CancellationToken cancellationToken = default)
    {
        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        if (_connectionFactory.SupportsSelectForUpdate)
        {
            await using var lockCommand =
                connection.CreateCommand();
            lockCommand.Transaction = transaction;
            lockCommand.CommandText =
                "SELECT pg_advisory_xact_lock(@lock_key)";
            AddParameter(
                lockCommand,
                "@lock_key",
                PersistenceMigrationLocks.SharedSchemaInfo);
            await lockCommand
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (var statement in InitializationStatements)
        {
            await ExecuteAsync(
                    connection,
                    transaction,
                    statement,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var existingVersion =
            await ReadSchemaVersionAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);

        if (existingVersion is not null &&
            existingVersion.Value != SchemaVersion)
        {
            throw new InvalidOperationException(
                $"Data-quality schema version {existingVersion.Value} is unsupported by this binary (expected {SchemaVersion}).");
        }

        await using var versionCommand =
            connection.CreateCommand();
        versionCommand.Transaction = transaction;
        versionCommand.CommandText =
            """
            INSERT INTO kafdeck_schema_info (
                component,
                schema_version)
            VALUES (
                @component,
                @schema_version)
            ON CONFLICT (component)
            DO NOTHING
            """;
        AddParameter(
            versionCommand,
            "@component",
            Component);
        AddParameter(
            versionCommand,
            "@schema_version",
            SchemaVersion);
        await versionCommand
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<DataQualityPolicyLifecycleSnapshot?>
        GetPolicyAsync(
            string policyId,
            CancellationToken cancellationToken = default)
    {
        ValidatePolicyId(policyId);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            """
            SELECT
                definition_json,
                lifecycle_state,
                revision,
                updated_at_utc
            FROM kafdeck_data_quality_policies
            WHERE policy_id = @policy_id
            """;
        AddParameter(command, "@policy_id", policyId);

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        return await reader
            .ReadAsync(cancellationToken)
            .ConfigureAwait(false)
            ? ReadPolicy(reader)
            : null;
    }

    public async Task<DataQualityPolicyPage>
        ListPoliciesAsync(
            DataQualityPolicyListQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        command.CommandText =
            """
            SELECT
                policy_id,
                definition_json,
                lifecycle_state,
                revision,
                updated_at_utc
            FROM kafdeck_data_quality_policies
            WHERE cluster_id = @cluster_id
              AND (@state IS NULL OR lifecycle_state = @state)
              AND (@after_policy_id IS NULL OR policy_id > @after_policy_id)
            ORDER BY policy_id
            LIMIT @row_limit
            """;
        AddParameter(
            command,
            "@cluster_id",
            query.ClusterId);
        AddParameter(
            command,
            "@state",
            query.State is null
                ? DBNull.Value
                : query.State.Value.ToString());
        AddParameter(
            command,
            "@after_policy_id",
            query.AfterPolicyId is null
                ? DBNull.Value
                : query.AfterPolicyId);
        AddParameter(
            command,
            "@row_limit",
            query.MaxResults + 1);

        var items =
            new List<DataQualityPolicyLifecycleSnapshot>(
                query.MaxResults);
        string? nextPolicyId = null;

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        while (await reader
                   .ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            if (items.Count >= query.MaxResults)
            {
                nextPolicyId =
                    reader.GetString(0);
                break;
            }

            items.Add(ReadPolicy(reader, offset: 1));
        }

        var truncated =
            nextPolicyId is not null;
        if (truncated && items.Count > 0)
        {
            nextPolicyId =
                items[^1].Definition.PolicyId;
        }

        return new DataQualityPolicyPage(
            items,
            truncated,
            nextPolicyId);
    }

    public async Task<DataQualityPolicyLifecycleSnapshot>
        CreatePolicyAsync(
            DataQualityPolicyDefinition definition,
            DataQualityPolicyLifecycleState state,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidateWrite(definition, state, updatedAtUtc);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            INSERT INTO kafdeck_data_quality_policies (
                policy_id,
                cluster_id,
                lifecycle_state,
                revision,
                updated_at_utc,
                definition_json)
            VALUES (
                @policy_id,
                @cluster_id,
                @state,
                1,
                @updated_at_utc,
                @definition_json)
            ON CONFLICT (policy_id)
            DO NOTHING
            """;
        BindPolicy(
            command,
            definition,
            state,
            updatedAtUtc);
        var affected =
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);

        if (affected != 1)
        {
            throw new InvalidOperationException(
                $"Data-quality policy '{definition.PolicyId}' already exists.");
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new DataQualityPolicyLifecycleSnapshot(
            definition,
            state,
            1,
            updatedAtUtc);
    }

    public Task<DataQualityPolicyLifecycleSnapshot?>
        ReplacePolicyAsync(
            DataQualityPolicyDefinition definition,
            DataQualityPolicyLifecycleState state,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default) =>
        UpdatePolicyAsync(
            definition,
            state,
            expectedRevision,
            updatedAtUtc,
            replaceDefinition: true,
            cancellationToken);

    public async Task<DataQualityPolicyLifecycleSnapshot?>
        SetPolicyStateAsync(
            string policyId,
            DataQualityPolicyLifecycleState state,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidatePolicyId(policyId);
        if (!Enum.IsDefined(state) ||
            expectedRevision < 1 ||
            updatedAtUtc == default)
        {
            throw new ArgumentException(
                "Data-quality lifecycle state update is invalid.");
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            UPDATE kafdeck_data_quality_policies
            SET
                lifecycle_state = @state,
                revision = revision + 1,
                updated_at_utc = @updated_at_utc
            WHERE policy_id = @policy_id
              AND revision = @expected_revision
            RETURNING
                definition_json,
                lifecycle_state,
                revision,
                updated_at_utc
            """;
        AddParameter(command, "@policy_id", policyId);
        AddParameter(command, "@state", state.ToString());
        AddParameter(command, "@expected_revision", expectedRevision);
        AddParameter(command, "@updated_at_utc", Format(updatedAtUtc));

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);
        if (!await reader
                .ReadAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        var snapshot = ReadPolicy(reader);
        await reader
            .DisposeAsync()
            .ConfigureAwait(false);
        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return snapshot;
    }

    public async Task AppendEvidenceAsync(
        DataQualityEvidencePoint point,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(point);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        var evidence = point.Evidence;
        var progress = point.Progress;

        command.CommandText =
            """
            INSERT INTO kafdeck_data_quality_evidence (
                policy_id,
                policy_version,
                cluster_id,
                topic_name,
                partition_id,
                window_start_utc,
                window_end_utc,
                start_offset,
                end_offset_exclusive,
                next_offset,
                evaluated_records,
                evaluated_bytes,
                violation_count,
                violations_json,
                evidence_state,
                outcome,
                source,
                updated_at_utc)
            VALUES (
                @policy_id,
                @policy_version,
                @cluster_id,
                @topic_name,
                @partition_id,
                @window_start_utc,
                @window_end_utc,
                @start_offset,
                @end_offset_exclusive,
                @next_offset,
                @evaluated_records,
                @evaluated_bytes,
                @violation_count,
                @violations_json,
                @evidence_state,
                @outcome,
                @source,
                @updated_at_utc)
            ON CONFLICT (
                policy_id,
                policy_version,
                cluster_id,
                topic_name,
                partition_id,
                window_start_utc,
                window_end_utc,
                start_offset,
                end_offset_exclusive)
            DO NOTHING
            """;

        AddParameter(command, "@policy_id", evidence.PolicyId);
        AddParameter(command, "@policy_version", evidence.PolicyVersion);
        AddParameter(command, "@cluster_id", progress.ClusterId);
        AddParameter(command, "@topic_name", progress.TopicName);
        AddParameter(command, "@partition_id", progress.Partition);
        AddParameter(command, "@window_start_utc", Format(evidence.WindowStartUtc));
        AddParameter(command, "@window_end_utc", Format(evidence.WindowEndUtc));
        AddParameter(command, "@start_offset", progress.StartOffset);
        AddParameter(command, "@end_offset_exclusive", progress.EndOffsetExclusive);
        AddParameter(command, "@next_offset", progress.NextOffset);
        AddParameter(command, "@evaluated_records", evidence.EvaluatedRecords);
        AddParameter(command, "@evaluated_bytes", evidence.EvaluatedBytes);
        AddParameter(command, "@violation_count", evidence.ViolationCount);
        AddParameter(
            command,
            "@violations_json",
            JsonSerializer.Serialize(
                evidence.ViolationsByRule,
                JsonOptions));
        AddParameter(command, "@evidence_state", evidence.State.ToString());
        AddParameter(command, "@outcome", progress.Outcome.ToString());
        AddParameter(command, "@source", evidence.Source);
        AddParameter(command, "@updated_at_utc", Format(progress.UpdatedAtUtc));

        await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<DataQualityEvidencePage>
        QueryEvidenceAsync(
            DataQualityEvidenceQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();

        command.CommandText =
            """
            SELECT
                policy_version,
                cluster_id,
                topic_name,
                partition_id,
                window_start_utc,
                window_end_utc,
                start_offset,
                end_offset_exclusive,
                next_offset,
                evaluated_records,
                evaluated_bytes,
                violation_count,
                violations_json,
                evidence_state,
                outcome,
                source,
                updated_at_utc
            FROM kafdeck_data_quality_evidence
            WHERE policy_id = @policy_id
              AND window_start_utc >= @from_utc
              AND window_start_utc < @to_utc
            ORDER BY
                window_start_utc,
                partition_id,
                start_offset,
                policy_version,
                cluster_id,
                topic_name,
                window_end_utc,
                end_offset_exclusive
            LIMIT @row_limit
            """;
        AddParameter(command, "@policy_id", query.PolicyId);
        AddParameter(command, "@from_utc", Format(query.FromUtc));
        AddParameter(command, "@to_utc", Format(query.ToUtc));
        AddParameter(command, "@row_limit", query.MaxPoints + 1);

        var points =
            new List<DataQualityEvidencePoint>(
                query.MaxPoints);
        var truncated = false;

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        while (await reader
                   .ReadAsync(cancellationToken)
                   .ConfigureAwait(false))
        {
            if (points.Count >= query.MaxPoints)
            {
                truncated = true;
                break;
            }

            points.Add(
                ReadEvidencePoint(
                    query.PolicyId,
                    reader));
        }

        return new DataQualityEvidencePage(
            points,
            truncated);
    }

    private async Task<DataQualityPolicyLifecycleSnapshot?>
        UpdatePolicyAsync(
            DataQualityPolicyDefinition definition,
            DataQualityPolicyLifecycleState state,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            bool replaceDefinition,
            CancellationToken cancellationToken)
    {
        ValidateWrite(definition, state, updatedAtUtc);
        if (expectedRevision < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedRevision));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var transaction =
            await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            replaceDefinition
                ? """
                  UPDATE kafdeck_data_quality_policies
                  SET
                      cluster_id = @cluster_id,
                      lifecycle_state = @state,
                      revision = revision + 1,
                      updated_at_utc = @updated_at_utc,
                      definition_json = @definition_json
                  WHERE policy_id = @policy_id
                    AND revision = @expected_revision
                  """
                : """
                  UPDATE kafdeck_data_quality_policies
                  SET
                      lifecycle_state = @state,
                      revision = revision + 1,
                      updated_at_utc = @updated_at_utc
                  WHERE policy_id = @policy_id
                    AND revision = @expected_revision
                  """;
        BindPolicy(
            command,
            definition,
            state,
            updatedAtUtc);
        AddParameter(
            command,
            "@expected_revision",
            expectedRevision);

        var affected =
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        if (affected != 1)
        {
            await transaction
                .RollbackAsync(cancellationToken)
                .ConfigureAwait(false);
            return null;
        }

        await transaction
            .CommitAsync(cancellationToken)
            .ConfigureAwait(false);

        return new DataQualityPolicyLifecycleSnapshot(
            definition,
            state,
            checked(expectedRevision + 1),
            updatedAtUtc);
    }

    private static DataQualityPolicyLifecycleSnapshot ReadPolicy(
        DbDataReader reader,
        int offset = 0)
    {
        var definition =
            DeserializeDefinition(
                reader.GetString(offset));

        var state =
            Enum.Parse<DataQualityPolicyLifecycleState>(
                reader.GetString(offset + 1),
                ignoreCase: false);
        var revision =
            Convert.ToInt64(
                reader.GetValue(offset + 2),
                CultureInfo.InvariantCulture);
        var updatedAtUtc =
            Parse(reader.GetString(offset + 3));

        return new DataQualityPolicyLifecycleSnapshot(
            definition,
            state,
            revision,
            updatedAtUtc);
    }

    private static DataQualityEvidencePoint ReadEvidencePoint(
        string policyId,
        DbDataReader reader)
    {
        var policyVersion =
            Convert.ToInt32(
                reader.GetValue(0),
                CultureInfo.InvariantCulture);
        var clusterId = reader.GetString(1);
        var topicName = reader.GetString(2);
        var partition =
            Convert.ToInt32(
                reader.GetValue(3),
                CultureInfo.InvariantCulture);
        var windowStartUtc = Parse(reader.GetString(4));
        var windowEndUtc = Parse(reader.GetString(5));
        var startOffset =
            Convert.ToInt64(
                reader.GetValue(6),
                CultureInfo.InvariantCulture);
        var endOffsetExclusive =
            Convert.ToInt64(
                reader.GetValue(7),
                CultureInfo.InvariantCulture);
        var nextOffset =
            Convert.ToInt64(
                reader.GetValue(8),
                CultureInfo.InvariantCulture);
        var evaluatedRecords =
            Convert.ToInt64(
                reader.GetValue(9),
                CultureInfo.InvariantCulture);
        var evaluatedBytes =
            Convert.ToInt64(
                reader.GetValue(10),
                CultureInfo.InvariantCulture);
        var violationCount =
            Convert.ToInt64(
                reader.GetValue(11),
                CultureInfo.InvariantCulture);
        var violations =
            JsonSerializer.Deserialize<
                DataQualityRuleViolationCount[]>(
                reader.GetString(12),
                JsonOptions) ??
            throw new InvalidOperationException(
                "Persisted data-quality violation breakdown is invalid.");
        var evidenceState =
            Enum.Parse<DataQualityEvidenceState>(
                reader.GetString(13),
                ignoreCase: false);
        var outcome =
            Enum.Parse<DataQualityEvaluationOutcome>(
                reader.GetString(14),
                ignoreCase: false);
        var source = reader.GetString(15);
        var updatedAtUtc = Parse(reader.GetString(16));

        var evidence =
            new DataQualityAggregateEvidence(
                policyId,
                policyVersion,
                windowStartUtc,
                windowEndUtc,
                evaluatedRecords,
                evaluatedBytes,
                violationCount,
                violations,
                evidenceState,
                source);
        var progress =
            new DataQualityEvaluationProgress(
                policyId,
                policyVersion,
                clusterId,
                topicName,
                partition,
                windowStartUtc,
                windowEndUtc,
                startOffset,
                endOffsetExclusive,
                nextOffset,
                evaluatedRecords,
                evaluatedBytes,
                evidenceState,
                outcome,
                updatedAtUtc);

        return new DataQualityEvidencePoint(
            new DataQualityEvaluationResult(
                evidence,
                progress));
    }

    private sealed class PolicyDocument
    {
        public string PolicyId { get; init; } = string.Empty;
        public int Version { get; init; }
        public ScopeDocument Scope { get; init; } = new();
        public RuleDocument[] Rules { get; init; } = [];
        public BudgetDocument Budget { get; init; } = new();
    }

    private sealed class ScopeDocument
    {
        public string ClusterId { get; init; } = string.Empty;
        public string TopicName { get; init; } = string.Empty;
        public int[] Partitions { get; init; } = [];
    }

    private sealed class RuleDocument
    {
        public string RuleId { get; init; } = string.Empty;
        public DataQualityRuleKind Kind { get; init; }
        public string JsonPointer { get; init; } = string.Empty;
        public DataQualityValueType? ExpectedType { get; init; }
        public double? MinimumNumber { get; init; }
        public double? MaximumNumber { get; init; }
        public int? MinimumLength { get; init; }
        public int? MaximumLength { get; init; }
    }

    private sealed class BudgetDocument
    {
        public int RecordsPerSecond { get; init; }
        public long BytesPerSecond { get; init; }
        public long EvaluationWindowTicks { get; init; }
        public int ActivePoliciesPerCluster { get; init; }
        public int ConcurrentReadersPerCluster { get; init; }
    }

    private static string SerializeDefinition(
        DataQualityPolicyDefinition definition)
    {
        var document =
            new PolicyDocument
            {
                PolicyId = definition.PolicyId,
                Version = definition.Version,
                Scope =
                    new ScopeDocument
                    {
                        ClusterId = definition.Scope.ClusterId,
                        TopicName = definition.Scope.TopicName,
                        Partitions = definition.Scope.Partitions.ToArray(),
                    },
                Rules =
                    definition.Rules
                        .Select(
                            rule =>
                                new RuleDocument
                                {
                                    RuleId = rule.RuleId,
                                    Kind = rule.Kind,
                                    JsonPointer = rule.JsonPointer,
                                    ExpectedType = rule.ExpectedType,
                                    MinimumNumber = rule.MinimumNumber,
                                    MaximumNumber = rule.MaximumNumber,
                                    MinimumLength = rule.MinimumLength,
                                    MaximumLength = rule.MaximumLength,
                                })
                        .ToArray(),
                Budget =
                    new BudgetDocument
                    {
                        RecordsPerSecond = definition.Budget.RecordsPerSecond,
                        BytesPerSecond = definition.Budget.BytesPerSecond,
                        EvaluationWindowTicks = definition.Budget.EvaluationWindow.Ticks,
                        ActivePoliciesPerCluster = definition.Budget.ActivePoliciesPerCluster,
                        ConcurrentReadersPerCluster = definition.Budget.ConcurrentReadersPerCluster,
                    },
            };

        return JsonSerializer.Serialize(
            document,
            JsonOptions);
    }

    private static DataQualityPolicyDefinition DeserializeDefinition(
        string json)
    {
        var document =
            JsonSerializer.Deserialize<PolicyDocument>(
                json,
                JsonOptions) ??
            throw new InvalidOperationException(
                "Persisted data-quality policy definition is invalid.");

        var rules =
            document.Rules
                .Select(
                    rule =>
                        new DataQualityRule(
                            rule.RuleId,
                            rule.Kind,
                            rule.JsonPointer,
                            rule.ExpectedType,
                            rule.MinimumNumber,
                            rule.MaximumNumber,
                            rule.MinimumLength,
                            rule.MaximumLength))
                .ToArray();

        return new DataQualityPolicyDefinition(
            document.PolicyId,
            document.Version,
            new DataQualityPolicyScope(
                document.Scope.ClusterId,
                document.Scope.TopicName,
                document.Scope.Partitions),
            rules,
            new DataQualityPolicyBudget(
                document.Budget.RecordsPerSecond,
                document.Budget.BytesPerSecond,
                TimeSpan.FromTicks(
                    document.Budget.EvaluationWindowTicks),
                document.Budget.ActivePoliciesPerCluster,
                document.Budget.ConcurrentReadersPerCluster));
    }

    private static void ValidatePolicyId(
        string policyId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(policyId);

        if (policyId.Length is < 1 or >
                DataQualityPolicyDefinition.MaxPolicyIdLength ||
            !string.Equals(
                policyId,
                policyId.Trim(),
                StringComparison.Ordinal) ||
            policyId.Any(char.IsControl))
        {
            throw new ArgumentException(
                "Data-quality policy ID must be an exact bounded identifier.",
                nameof(policyId));
        }
    }

    private static void ValidateWrite(
        DataQualityPolicyDefinition definition,
        DataQualityPolicyLifecycleState state,
        DateTimeOffset updatedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (!Enum.IsDefined(state) ||
            updatedAtUtc == default)
        {
            throw new ArgumentException(
                "Data-quality lifecycle write is invalid.");
        }
    }

    private static void BindPolicy(
        DbCommand command,
        DataQualityPolicyDefinition definition,
        DataQualityPolicyLifecycleState state,
        DateTimeOffset updatedAtUtc)
    {
        AddParameter(command, "@policy_id", definition.PolicyId);
        AddParameter(command, "@cluster_id", definition.Scope.ClusterId);
        AddParameter(command, "@state", state.ToString());
        AddParameter(command, "@updated_at_utc", Format(updatedAtUtc));
        AddParameter(
            command,
            "@definition_json",
            SerializeDefinition(definition));
    }

    private static readonly string[] InitializationStatements =
    [
        """
        CREATE TABLE IF NOT EXISTS kafdeck_schema_info (
            component TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_data_quality_policies (
            policy_id TEXT PRIMARY KEY,
            cluster_id TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL,
            revision BIGINT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            definition_json TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_data_quality_policy_list
        ON kafdeck_data_quality_policies (
            cluster_id,
            lifecycle_state,
            policy_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_data_quality_evidence (
            policy_id TEXT NOT NULL,
            policy_version INTEGER NOT NULL,
            cluster_id TEXT NOT NULL,
            topic_name TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            window_start_utc TEXT NOT NULL,
            window_end_utc TEXT NOT NULL,
            start_offset BIGINT NOT NULL,
            end_offset_exclusive BIGINT NOT NULL,
            next_offset BIGINT NOT NULL,
            evaluated_records BIGINT NOT NULL,
            evaluated_bytes BIGINT NOT NULL,
            violation_count BIGINT NOT NULL,
            violations_json TEXT NOT NULL,
            evidence_state TEXT NOT NULL,
            outcome TEXT NOT NULL,
            source TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (
                policy_id,
                policy_version,
                cluster_id,
                topic_name,
                partition_id,
                window_start_utc,
                window_end_utc,
                start_offset,
                end_offset_exclusive)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_data_quality_evidence_lookup
        ON kafdeck_data_quality_evidence (
            policy_id,
            window_start_utc,
            partition_id,
            start_offset)
        """,
    ];

    private static async Task ExecuteAsync(
        DbConnection connection,
        DbTransaction transaction,
        string statement,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = statement;
        await command
            .ExecuteNonQueryAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<int?> ReadSchemaVersionAsync(
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = @component
            """;
        AddParameter(command, "@component", Component);

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return value is null || value is DBNull
            ? null
            : Convert.ToInt32(
                value,
                CultureInfo.InvariantCulture);
    }

    private static DateTimeOffset Parse(string value) =>
        DateTimeOffset.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind);

    private static string Format(DateTimeOffset value) =>
        value.ToUniversalTime().ToString("O");

    private static void AddParameter(
        DbCommand command,
        string name,
        object value)
    {
        var parameter =
            command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
