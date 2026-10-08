using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

public sealed class AdoNotificationDestinationStore :
    INotificationDestinationStore
{
    private const int SchemaVersion = 1;
    private const string Component =
        "notification-destinations";

    private static readonly JsonSerializerOptions
        JsonOptions =
        new(JsonSerializerDefaults.Web);

    private readonly INotificationDeliveryDbConnectionFactory
        _connectionFactory;

    public AdoNotificationDestinationStore(
        INotificationDeliveryDbConnectionFactory connectionFactory)
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
            lockCommand.Transaction =
                transaction;
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
            await using var command =
                connection.CreateCommand();
            command.Transaction =
                transaction;
            command.CommandText =
                statement;
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        var existing =
            await ReadSchemaVersionAsync(
                    connection,
                    transaction,
                    cancellationToken)
                .ConfigureAwait(false);

        if (existing is not null &&
            existing.Value != SchemaVersion)
        {
            throw new InvalidOperationException(
                $"Notification-destination schema version {existing.Value} is unsupported by this binary (expected {SchemaVersion}).");
        }

        await using var versionCommand =
            connection.CreateCommand();
        versionCommand.Transaction =
            transaction;
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

    public async Task<NotificationDestinationRecord?>
        GetAsync(
            string destinationId,
            CancellationToken cancellationToken = default)
    {
        var normalized =
            NotificationDestinationProfile.NormalizeDestinationId(
                destinationId);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            SelectColumns +
            """
            
            FROM kafdeck_notification_destinations
            WHERE destination_id = @destination_id
            """;
        AddParameter(
            command,
            "@destination_id",
            normalized);

        await using var reader =
            await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);

        return await reader
            .ReadAsync(cancellationToken)
            .ConfigureAwait(false)
            ? ReadRecord(reader)
            : null;
    }

    public async Task<NotificationDestinationRecord>
        CreateAsync(
            NotificationDestinationDefinition definition,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidateWrite(
            definition,
            updatedAtUtc);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            """
            INSERT INTO kafdeck_notification_destinations (
                destination_id,
                provider,
                display_name,
                enabled_events_json,
                configured_endpoint,
                credential_binding_id,
                provider_target,
                lifecycle_state,
                revision,
                created_at_utc,
                updated_at_utc)
            VALUES (
                @destination_id,
                @provider,
                @display_name,
                @enabled_events_json,
                @configured_endpoint,
                @credential_binding_id,
                @provider_target,
                @lifecycle_state,
                1,
                @created_at_utc,
                @updated_at_utc)
            ON CONFLICT (destination_id)
            DO NOTHING
            """;
        BindDefinition(
            command,
            definition,
            updatedAtUtc);

        var affected =
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);

        if (affected != 1)
        {
            throw new InvalidOperationException(
                $"Notification destination '{definition.Profile.DestinationId}' already exists.");
        }

        return new NotificationDestinationRecord(
            definition,
            1,
            updatedAtUtc);
    }

    public async Task<NotificationDestinationRecord?>
        ReplaceAsync(
            NotificationDestinationDefinition definition,
            long expectedRevision,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken = default)
    {
        ValidateWrite(
            definition,
            updatedAtUtc);

        if (expectedRevision < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(expectedRevision));
        }

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            """
            UPDATE kafdeck_notification_destinations
            SET
                provider = @provider,
                display_name = @display_name,
                enabled_events_json = @enabled_events_json,
                configured_endpoint = @configured_endpoint,
                credential_binding_id = @credential_binding_id,
                provider_target = @provider_target,
                lifecycle_state = @lifecycle_state,
                revision = revision + 1,
                updated_at_utc = @updated_at_utc
            WHERE destination_id = @destination_id
              AND revision = @expected_revision
              AND created_at_utc = @created_at_utc
            """;
        BindDefinition(
            command,
            definition,
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
            return null;
        }

        return new NotificationDestinationRecord(
            definition,
            checked(expectedRevision + 1),
            updatedAtUtc);
    }

    public async Task<NotificationDestinationPage>
        ListAsync(
            NotificationDestinationListQuery query,
            CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        var predicates =
            new List<string>();

        if (query.Provider is not null)
        {
            predicates.Add(
                "provider = @provider");
        }

        if (query.State is not null)
        {
            predicates.Add(
                "lifecycle_state = @lifecycle_state");
        }

        if (query.AfterDestinationId is not null)
        {
            predicates.Add(
                "destination_id > @after_destination_id");
        }

        var where =
            predicates.Count == 0
                ? string.Empty
                : " WHERE " +
                  string.Join(
                      " AND ",
                      predicates);

        await using var connection =
            await _connectionFactory
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);
        await using var command =
            connection.CreateCommand();
        command.CommandText =
            SelectColumns +
            "\nFROM kafdeck_notification_destinations" +
            where +
            """
            
            ORDER BY destination_id
            LIMIT @row_limit
            """;

        if (query.Provider is not null)
        {
            AddParameter(
                command,
                "@provider",
                (int)query.Provider.Value);
        }

        if (query.State is not null)
        {
            AddParameter(
                command,
                "@lifecycle_state",
                query.State.Value.ToString());
        }

        if (query.AfterDestinationId is not null)
        {
            AddParameter(
                command,
                "@after_destination_id",
                query.AfterDestinationId);
        }

        AddParameter(
            command,
            "@row_limit",
            query.MaxResults + 1);

        var items =
            new List<NotificationDestinationRecord>(
                query.MaxResults);
        var truncated =
            false;

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
                truncated =
                    true;
                break;
            }

            items.Add(
                ReadRecord(reader));
        }

        return new NotificationDestinationPage(
            items,
            truncated,
            truncated
                ? items[^1].Definition.Profile.DestinationId
                : null);
    }

    private static void ValidateWrite(
        NotificationDestinationDefinition definition,
        DateTimeOffset updatedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(definition);

        if (updatedAtUtc == default ||
            updatedAtUtc <
                definition.CreatedAtUtc)
        {
            throw new ArgumentException(
                "Notification destination update timestamp is invalid.",
                nameof(updatedAtUtc));
        }
    }

    private static void BindDefinition(
        DbCommand command,
        NotificationDestinationDefinition definition,
        DateTimeOffset updatedAtUtc)
    {
        var profile =
            definition.Profile;

        AddParameter(
            command,
            "@destination_id",
            profile.DestinationId);
        AddParameter(
            command,
            "@provider",
            (int)profile.Provider);
        AddParameter(
            command,
            "@display_name",
            profile.DisplayName);
        AddParameter(
            command,
            "@enabled_events_json",
            JsonSerializer.Serialize(
                profile.EnabledEvents,
                JsonOptions));
        AddParameter(
            command,
            "@configured_endpoint",
            profile.ConfiguredEndpoint?.AbsoluteUri ??
            DBNull.Value);
        AddParameter(
            command,
            "@credential_binding_id",
            profile.CredentialBindingId?.Value ??
            DBNull.Value);
        AddParameter(
            command,
            "@provider_target",
            definition.ProviderTarget ??
            DBNull.Value);
        AddParameter(
            command,
            "@lifecycle_state",
            definition.State.ToString());
        AddParameter(
            command,
            "@created_at_utc",
            Format(definition.CreatedAtUtc));
        AddParameter(
            command,
            "@updated_at_utc",
            Format(updatedAtUtc));
    }

    private static NotificationDestinationRecord
        ReadRecord(
            DbDataReader reader)
    {
        var destinationId =
            reader.GetString(0);
        var provider =
            (NotificationProviderKind)Convert.ToInt32(
                reader.GetValue(1),
                CultureInfo.InvariantCulture);
        var displayName =
            reader.GetString(2);
        var events =
            JsonSerializer.Deserialize<
                NotificationEventClass[]>(
                reader.GetString(3),
                JsonOptions) ??
            throw new InvalidOperationException(
                "Persisted notification destination events are invalid.");

        Uri? endpoint =
            reader.IsDBNull(4)
                ? null
                : new Uri(
                    reader.GetString(4),
                    UriKind.Absolute);
        NotificationCredentialBindingId? binding =
            reader.IsDBNull(5)
                ? null
                : new NotificationCredentialBindingId(
                    reader.GetString(5));
        var providerTarget =
            reader.IsDBNull(6)
                ? null
                : reader.GetString(6);
        var state =
            Enum.Parse<NotificationDestinationLifecycleState>(
                reader.GetString(7),
                ignoreCase: false);
        var revision =
            Convert.ToInt64(
                reader.GetValue(8),
                CultureInfo.InvariantCulture);
        var createdAt =
            Parse(reader.GetString(9));
        var updatedAt =
            Parse(reader.GetString(10));

        var profile =
            new NotificationDestinationProfile(
                destinationId,
                provider,
                displayName,
                events,
                endpoint,
                binding);

        return new NotificationDestinationRecord(
            new NotificationDestinationDefinition(
                profile,
                state,
                createdAt,
                providerTarget),
            revision,
            updatedAt);
    }

    private static async Task<int?>
        ReadSchemaVersionAsync(
            DbConnection connection,
            DbTransaction transaction,
            CancellationToken cancellationToken)
    {
        await using var command =
            connection.CreateCommand();
        command.Transaction =
            transaction;
        command.CommandText =
            """
            SELECT schema_version
            FROM kafdeck_schema_info
            WHERE component = @component
            """;
        AddParameter(
            command,
            "@component",
            Component);

        var value =
            await command
                .ExecuteScalarAsync(cancellationToken)
                .ConfigureAwait(false);

        return value is null ||
               value is DBNull
            ? null
            : Convert.ToInt32(
                value,
                CultureInfo.InvariantCulture);
    }

    private static DateTimeOffset Parse(
        string value) =>
        DateTimeOffset.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind);

    private static string Format(
        DateTimeOffset value) =>
        value
            .ToUniversalTime()
            .ToString(
                "O",
                CultureInfo.InvariantCulture);

    private static void AddParameter(
        DbCommand command,
        string name,
        object value)
    {
        var parameter =
            command.CreateParameter();
        parameter.ParameterName =
            name;
        parameter.Value =
            value;
        command.Parameters.Add(parameter);
    }

    private const string SelectColumns =
        """
        SELECT
            destination_id,
            provider,
            display_name,
            enabled_events_json,
            configured_endpoint,
            credential_binding_id,
            provider_target,
            lifecycle_state,
            revision,
            created_at_utc,
            updated_at_utc
        """;

    private static readonly string[] InitializationStatements =
    [
        """
        CREATE TABLE IF NOT EXISTS kafdeck_schema_info (
            component TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS kafdeck_notification_destinations (
            destination_id TEXT PRIMARY KEY,
            provider INTEGER NOT NULL
                CHECK (provider BETWEEN 1 AND 6),
            display_name TEXT NOT NULL,
            enabled_events_json TEXT NOT NULL,
            configured_endpoint TEXT NULL,
            credential_binding_id TEXT NULL,
            provider_target TEXT NULL,
            lifecycle_state TEXT NOT NULL
                CHECK (lifecycle_state IN ('Active', 'Paused', 'Retired')),
            revision BIGINT NOT NULL
                CHECK (revision >= 1),
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_destination_provider
        ON kafdeck_notification_destinations (
            provider,
            destination_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_kafdeck_notification_destination_state
        ON kafdeck_notification_destinations (
            lifecycle_state,
            destination_id)
        """,
    ];
}
