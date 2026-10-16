namespace Kafdeck.Core.Notifications;

/// <summary>Optional operator interlock independent of restored delivery rows.</summary>
public interface INotificationDeliveryActivationGate
{
    ValueTask VerifyAsync(CancellationToken cancellationToken);
}

/// <summary>Reads an offline-enrolled epoch; never creates or advances it.</summary>
public interface INotificationDeliveryGenerationReader
{
    ValueTask<Guid?> ReadAsync(CancellationToken cancellationToken);
}
