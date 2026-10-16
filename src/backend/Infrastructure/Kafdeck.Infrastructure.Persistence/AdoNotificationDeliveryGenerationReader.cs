using System.Data.Common;
using Kafdeck.Core.Notifications;

namespace Kafdeck.Infrastructure.Persistence;

/// <summary>Reads a separately offline-provisioned table. No enrollment or migration.</summary>
public sealed class AdoNotificationDeliveryGenerationReader(INotificationDeliveryDbConnectionFactory connections)
    : INotificationDeliveryGenerationReader
{
    public async ValueTask<Guid?> ReadAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await connections.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT generation FROM notification_delivery_generation WHERE id = 1";
            var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return value is not null && Guid.TryParseExact(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture),
                "D", out var generation) && generation != Guid.Empty ? generation : null;
        }
        catch (DbException) { return null; }
    }
}
