using System.Data.Common;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Attribution;
using Kafdeck.Core.Governance;
using Kafdeck.Infrastructure.Persistence;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

internal static class V09W82ReferenceCorpus
{
    internal const int RequestCount = 10_000;
    internal const int WindowCount = 43_200;

    internal static async Task<string> SeedAsync(W77Laboratory lab, W77GovernanceDataset seed,
        Func<Task> refresh, HttpClient client, CancellationToken token)
    {
        IMutationDbConnectionFactory factory = lab.Database is null
            ? new SqliteMutationDbConnectionFactory(lab.SqlitePath)
            : new PostgreSqlMutationDbConnectionFactory(lab.Database.ConnectionString);
        await using var connection = await factory.OpenAsync(token);
        var originalRequest = JsonSerializer.Deserialize<ServiceRequestRecord>(
            await ReadBodyAsync(connection, "SELECT body FROM service_request_heads WHERE request_id=@id", seed.RequestId, token),
            ServiceRequestContract.Json)!;
        var originalWindow = JsonSerializer.Deserialize<MeasurementWindow>(
            await ReadBodyAsync(connection, "SELECT body FROM attribution_measurements WHERE window_id=@id", seed.WindowId, token),
            AttributionContract.Json)!;
        var end = originalWindow.WindowStartUtc;
        var start = end.AddMinutes(-WindowCount);

        // Historical records are fixture data. The one live approved request and measured window
        // remain as controls, and the timed workload goes through the actual authenticated API.
        foreach (var indices in Enumerable.Range(1, RequestCount - 1).Chunk(200))
        {
            await using var transaction = await connection.BeginTransactionAsync(token);
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO service_request_heads(request_id,cluster_id,operation_id,version,revision,state,requester,body)
                  VALUES(@id,@cluster,@target,1,1,@state,@requester,@body);
                INSERT INTO service_request_revisions(request_id,revision,body) VALUES(@id,1,@body);
                INSERT INTO service_request_events(request_id,version,operation_id,body) VALUES(@id,1,@event_id,@event_body);
                """;
            var id = Parameter(insert, "id", "");
            Parameter(insert, "cluster", originalRequest.ClusterId);
            var target = Parameter(insert, "target", "");
            Parameter(insert, "state", (int)ServiceRequestState.Cancelled);
            Parameter(insert, "requester", originalRequest.RequesterPrincipalId);
            var body = Parameter(insert, "body", "");
            var eventId = Parameter(insert, "event_id", "");
            var eventBody = Parameter(insert, "event_body", "");
            foreach (var index in indices)
            {
                var timestamp = start.AddMinutes(index * 4);
                var operation = StableGuid("target", index);
                var history = StableGuid("event", index);
                var request = originalRequest with
                {
                    RequestId = StableGuid("request", index).ToString("N"),
                    Revision = 1,
                    Version = 1,
                    BusinessState = ServiceRequestState.Cancelled,
                    Binding = originalRequest.Binding with { OperationId = operation, OperationExpiresAtUtc = timestamp.AddMinutes(15) },
                    Reason = "W82 reference historical cancellation",
                    CreatedAtUtc = timestamp,
                    UpdatedAtUtc = timestamp.AddMinutes(1),
                    ExpiresAtUtc = timestamp.AddMinutes(15),
                    Decisions = []
                };
                id.Value = request.RequestId;
                target.Value = operation.ToString("D");
                body.Value = JsonSerializer.Serialize(request, ServiceRequestContract.Json);
                eventId.Value = history.ToString("D");
                eventBody.Value = JsonSerializer.Serialize(
                    new ServiceRequestHistory(request, request.RequesterPrincipalId, history, request.UpdatedAtUtc), ServiceRequestContract.Json);
                await insert.ExecuteNonQueryAsync(token);
            }
            await transaction.CommitAsync(token);
        }

        var source = new AttributionSourceIdentity("w82-reference-history", originalWindow.ClusterId, originalWindow.Provider);
        var scope = AttributionContract.AccessScope(originalWindow);
        foreach (var indices in Enumerable.Range(0, WindowCount).Chunk(500))
        {
            await using var transaction = await connection.BeginTransactionAsync(token);
            await using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO attribution_measurements(window_id,cluster_id,resource_id,starts_utc,ends_utc,source_id,content_hash,body,scope_id)
                  VALUES(@id,@cluster,@resource,@from,@to,@source,@hash,@body,@scope)
                """;
            var id = Parameter(insert, "id", "");
            Parameter(insert, "cluster", scope.ClusterId);
            Parameter(insert, "resource", scope.ResourceId);
            var fromUtc = Parameter(insert, "from", "");
            var toUtc = Parameter(insert, "to", "");
            Parameter(insert, "source", source.SourceId);
            var hash = Parameter(insert, "hash", "");
            var body = Parameter(insert, "body", "");
            Parameter(insert, "scope", scope.ScopeId);
            foreach (var index in indices)
            {
                var from = start.AddMinutes(index);
                var to = from.AddMinutes(1);
                var quantity = ((index % 64 + 1) * 1024).ToString(CultureInfo.InvariantCulture);
                var sample = new MeasuredCounterWindow(originalWindow.ResourceId, originalWindow.MetricKind, originalWindow.Unit,
                    from, to, "0", quantity, "Complete", "w82-history-v1", originalWindow.ResourceIdentity);
                var window = AttributionContract.Counter(source, sample, to, "w82-reference-history-v1") with { Ownership = originalWindow.Ownership };
                AttributionContract.Window(window);
                var serialized = JsonSerializer.Serialize(window, AttributionContract.Json);
                id.Value = window.WindowId;
                fromUtc.Value = from.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                toUtc.Value = to.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                hash.Value = AttributionContract.Hash(serialized);
                body.Value = serialized;
                await insert.ExecuteNonQueryAsync(token);
            }
            await transaction.CommitAsync(token);
        }

        const string prefix = "/api/v1/governance/";
        var range = $"&resourceId={Uri.EscapeDataString(seed.Topic)}&fromUtc={Uri.EscapeDataString(start.ToString("O"))}" +
            $"&toUtc={Uri.EscapeDataString(end.ToString("O"))}";
        await VerifyPagesAsync(client, prefix + "requests?clusterId=permitted&pageSize=200", "requestId", RequestCount, refresh, token);
        await VerifyPagesAsync(client, prefix + "measurements?clusterId=permitted&pageSize=200" + range, "windowId", WindowCount, refresh, token);
        return range;
    }

    private static async Task VerifyPagesAsync(HttpClient client, string route, string idProperty, int expected,
        Func<Task> refresh, CancellationToken token)
    {
        var cursors = new HashSet<string>(StringComparer.Ordinal);
        var ids = new HashSet<string>(StringComparer.Ordinal);
        string? cursor = null;
        do
        {
            await refresh();
            var address = cursor is null ? route : route + "&cursor=" + Uri.EscapeDataString(cursor);
            var page = await V09W82MixedLoadPilot.SendAsync(client, HttpMethod.Get, address, null, null, [], token);
            foreach (var item in page.GetProperty("items").EnumerateArray())
                Assert.True(ids.Add(item.GetProperty(idProperty).GetString()!), "Reference corpus page repeated an item.");
            Assert.InRange(ids.Count, 1, expected);
            cursor = page.TryGetProperty("nextCursor", out var next) ? next.GetString() : null;
            if (cursor is not null) Assert.True(cursors.Add(cursor), "Reference corpus cursor repeated.");
        } while (cursor is not null);
        Assert.Equal(expected, ids.Count);
    }

    private static Guid StableGuid(string kind, int index) =>
        new(SHA256.HashData(Encoding.UTF8.GetBytes($"w82-reference-{kind}-{index}")).AsSpan(0, 16));

    private static DbParameter Parameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
        return parameter;
    }

    private static async Task<string> ReadBodyAsync(DbConnection connection, string query, string id, CancellationToken token)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = query;
        Parameter(command, "id", id);
        return Assert.IsType<string>(await command.ExecuteScalarAsync(token));
    }
}
