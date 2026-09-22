using System.Globalization;
using System.Text.Json;
using Kafdeck.Core.Ecosystem;
using Kafdeck.Core.ReadViews;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Modules.Connect;

public sealed record ConnectMutationVerificationPolicy
{
    public ConnectMutationVerificationPolicy(
        TimeSpan timeout,
        TimeSpan pollInterval)
    {
        if (timeout < TimeSpan.FromSeconds(1) ||
            timeout > TimeSpan.FromSeconds(15))
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        if (pollInterval < TimeSpan.FromMilliseconds(50) ||
            pollInterval > TimeSpan.FromSeconds(2) ||
            pollInterval >= timeout)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }

        Timeout = timeout;
        PollInterval = pollInterval;
    }

    public TimeSpan Timeout { get; }
    public TimeSpan PollInterval { get; }

    public static ConnectMutationVerificationPolicy Default { get; } =
        new(
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(250));
}

public sealed class ConnectMutationExecutionService
{
    private static readonly TimeSpan ExecutionCompletionReserve =
        TimeSpan.FromMilliseconds(250);

    private readonly IConnectMutationPort _mutations;
    private readonly IConnectMutationObservationPort _observations;
    private readonly ConnectMutationVerificationPolicy _verification;
    private readonly TimeProvider _timeProvider;

    public ConnectMutationExecutionService(
        IConnectMutationPort mutations,
        IConnectMutationObservationPort observations,
        ConnectMutationVerificationPolicy? verification = null,
        TimeProvider? timeProvider = null)
    {
        _mutations = mutations ?? throw new ArgumentNullException(nameof(mutations));
        _observations = observations ?? throw new ArgumentNullException(nameof(observations));
        _verification =
            verification ?? ConnectMutationVerificationPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async Task<MutationProviderResult> CreateAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind !=
            MutationOperationKind.ConnectCreate)
        {
            return Unknown("connect_create_operation_mismatch");
        }

        ConnectCreateCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectCreateCanonicalIntent>(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return Unknown("connect_create_canonical_invalid");
        }

        if (!TryBuildCreateMutation(
                canonical,
                context.Material,
                out var mutation))
        {
            return Unknown("connect_create_material_invalid");
        }

        var accepted = await InvokeAsync(
                () => _mutations.CreateAsync(
                    mutation!,
                    cancellationToken),
                "connect_create",
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        var verification = await VerifyUntilAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                observation =>
                    observation.Exists &&
                    string.Equals(
                        observation.ConfigurationFingerprint,
                        canonical.RequestedConfigurationFingerprint,
                        StringComparison.Ordinal),
                cancellationToken,
                context.ExecutionDeadlineUtc)
            .ConfigureAwait(false);

        return verification.Verified
            ? Verified(
                "connect_create_verified",
                accepted,
                verification.Observation)
            : AppliedUnverified(
                "connect_create_verification_inconclusive",
                accepted,
                verification.Observation);
    }

    public async Task<MutationProviderResult> UpdateAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        ConnectUpdateCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectUpdateCanonicalIntent>(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return Unknown("connect_update_canonical_invalid");
        }

        if (canonical.AlterKind !=
                ConnectAlterIntentKind.ConfigurationUpdate ||
            !TryBuildUpdateMutation(
                canonical,
                context.Material,
                out var mutation))
        {
            return Unknown("connect_update_material_invalid");
        }

        var accepted = await InvokeAsync(
                () => _mutations.AlterAsync(
                    mutation!,
                    cancellationToken),
                "connect_update",
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        var verification = await VerifyUntilAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                observation =>
                    observation.Exists &&
                    string.Equals(
                        observation.ConfigurationFingerprint,
                        canonical.RequestedConfigurationFingerprint,
                        StringComparison.Ordinal),
                cancellationToken,
                context.ExecutionDeadlineUtc)
            .ConfigureAwait(false);

        return verification.Verified
            ? Verified(
                "connect_update_verified",
                accepted,
                verification.Observation)
            : AppliedUnverified(
                "connect_update_verification_inconclusive",
                accepted,
                verification.Observation);
    }

    public async Task<MutationProviderResult> ControlAsync(
        ConnectControlCanonicalIntent canonical,
        CancellationToken cancellationToken = default,
        DateTimeOffset? executionDeadlineUtc = null)
    {
        ArgumentNullException.ThrowIfNull(canonical);

        if (canonical.AlterKind != ConnectAlterIntentKind.Control ||
            !TryBuildControlMutation(
                canonical,
                out var mutation))
        {
            return Unknown("connect_control_canonical_invalid");
        }

        var operationCode = canonical.Action switch
        {
            ConnectControlAction.Pause =>
                "connect_pause",
            ConnectControlAction.Resume =>
                "connect_resume",
            ConnectControlAction.Restart
                when canonical.TaskId.HasValue =>
                "connect_task_restart",
            ConnectControlAction.Restart =>
                "connect_restart",
            _ => "connect_control",
        };

        var accepted = await InvokeAsync(
                () => _mutations.ControlAsync(
                    mutation!,
                    cancellationToken),
                operationCode,
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        if (canonical.Action == ConnectControlAction.Restart)
        {
            // A RUNNING readback is not proof that restart occurred, because the
            // connector/task may have been RUNNING before the request.
            return AppliedUnverified(
                $"{operationCode}_verification_inconclusive",
                accepted,
                observation: null);
        }

        var expectedState =
            canonical.Action == ConnectControlAction.Pause
                ? "PAUSED"
                : "RUNNING";

        var verification = await VerifyUntilAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                observation =>
                    observation.Exists &&
                    string.Equals(
                        observation.State,
                        expectedState,
                        StringComparison.Ordinal),
                cancellationToken,
                executionDeadlineUtc)
            .ConfigureAwait(false);

        return verification.Verified
            ? Verified(
                $"{operationCode}_verified",
                accepted,
                verification.Observation)
            : AppliedUnverified(
                $"{operationCode}_verification_inconclusive",
                accepted,
                verification.Observation);
    }

    public async Task<MutationProviderResult> DeleteAsync(
        ConnectDeleteCanonicalIntent canonical,
        CancellationToken cancellationToken = default,
        DateTimeOffset? executionDeadlineUtc = null)
    {
        ArgumentNullException.ThrowIfNull(canonical);

        if (!TryBuildDeleteMutation(
                canonical,
                out var mutation))
        {
            return Unknown("connect_delete_canonical_invalid");
        }

        var accepted = await InvokeAsync(
                () => _mutations.DeleteAsync(
                    mutation!,
                    cancellationToken),
                "connect_delete",
                cancellationToken)
            .ConfigureAwait(false);

        if (accepted.ResultKind !=
            MutationExecutionResultKind.AppliedUnverified)
        {
            return accepted;
        }

        var verification = await VerifyUntilAsync(
                canonical.ClusterId,
                canonical.ConnectorName,
                observation => !observation.Exists,
                cancellationToken,
                executionDeadlineUtc)
            .ConfigureAwait(false);

        return verification.Verified
            ? Verified(
                "connect_delete_verified",
                accepted,
                verification.Observation)
            : AppliedUnverified(
                "connect_delete_verification_inconclusive",
                accepted,
                verification.Observation);
    }

    private async Task<MutationProviderResult> InvokeAsync(
        Func<Task<MutationProviderResult>> action,
        string operationCode,
        CancellationToken cancellationToken)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return Unknown(
                $"{operationCode}_cancelled_or_timeout");
        }
        catch
        {
            return Unknown(
                $"{operationCode}_provider_exception");
        }
    }

    private async Task<ConnectVerificationObservation> VerifyUntilAsync(
        string clusterId,
        string connectorName,
        Func<ConnectMutationObservation, bool> verify,
        CancellationToken cancellationToken,
        DateTimeOffset? executionDeadlineUtc = null)
    {
        var now = _timeProvider.GetUtcNow();
        var deadline = now.Add(_verification.Timeout);

        if (executionDeadlineUtc.HasValue)
        {
            var safeExecutionDeadline =
                executionDeadlineUtc.Value - ExecutionCompletionReserve;
            if (safeExecutionDeadline < deadline)
            {
                deadline = safeExecutionDeadline;
            }
        }

        ConnectMutationObservation? last = null;
        if (deadline <= now || cancellationToken.IsCancellationRequested)
        {
            return new(false, last);
        }

        using var verificationCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        verificationCancellation.CancelAfter(deadline - now);
        var verificationToken = verificationCancellation.Token;

        while (_timeProvider.GetUtcNow() < deadline)
        {
            if (verificationToken.IsCancellationRequested)
            {
                return new(false, last);
            }

            try
            {
                var observed =
                    await _observations.ObserveConnectorAsync(
                            clusterId,
                            connectorName,
                            new ReadViewOperationContext(
                                deadline,
                                maxItems: 256,
                                maxResponseBytes:
                                    ConnectMutationPolicy
                                        .HardMaxConfigurationBytes * 2L),
                            verificationToken)
                        .ConfigureAwait(false);

                if (observed.IsSuccess &&
                    observed.Value is not null)
                {
                    last = observed.Value;
                    if (verify(observed.Value))
                    {
                        return new(true, observed.Value);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                return new(false, last);
            }
            catch
            {
                // Provider acceptance is already known. Readback errors are
                // evidence gaps, never proof that the mutation failed.
            }

            var remaining =
                deadline - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
                break;

            var delay =
                remaining < _verification.PollInterval
                    ? remaining
                    : _verification.PollInterval;

            try
            {
                await Task.Delay(
                        delay,
                        _timeProvider,
                        verificationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return new(false, last);
            }
        }

        return new(false, last);
    }

    private static bool TryBuildCreateMutation(
        ConnectCreateCanonicalIntent canonical,
        MutationExecutionMaterial material,
        out ConnectCreateMutation? mutation)
    {
        mutation = null;

        if (!ValidateRequestedConfiguration(
                canonical.MaterialName,
                canonical.RequestedConfigurationFingerprint,
                canonical.RequestedConfiguration))
        {
            return false;
        }

        try
        {
            var cluster =
                ConnectMutationCanonicalization.RequireIdentifier(
                    canonical.ClusterId,
                    "Cluster ID",
                    256);
            var connector =
                ConnectMutationCanonicalization.RequireConnectorName(
                    canonical.ConnectorName);
            var encoded =
                material.GetRequired(
                    canonical.MaterialName);
            var configuration =
                ConnectMutationCanonicalization.DecodeConfiguration(
                    encoded);
            var projected =
                ConnectMutationCanonicalization.ProjectConfiguration(
                    configuration);
            var fingerprint =
                ConnectMutationCanonicalization.ConfigurationFingerprint(
                    projected);

            if (!string.Equals(
                    fingerprint,
                    canonical.RequestedConfigurationFingerprint,
                    StringComparison.Ordinal))
            {
                return false;
            }

            mutation = new ConnectCreateMutation(
                cluster,
                connector,
                configuration);
            return true;
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                KeyNotFoundException or
                MutationStateException or
                JsonException)
        {
            return false;
        }
    }

    private static bool TryBuildUpdateMutation(
        ConnectUpdateCanonicalIntent canonical,
        MutationExecutionMaterial material,
        out ConnectAlterMutation? mutation)
    {
        mutation = null;

        if (!ValidateRequestedConfiguration(
                canonical.MaterialName,
                canonical.RequestedConfigurationFingerprint,
                canonical.RequestedConfiguration) ||
            canonical.Diff.Count < 1)
        {
            return false;
        }

        try
        {
            var cluster =
                ConnectMutationCanonicalization.RequireIdentifier(
                    canonical.ClusterId,
                    "Cluster ID",
                    256);
            var connector =
                ConnectMutationCanonicalization.RequireConnectorName(
                    canonical.ConnectorName);
            var encoded =
                material.GetRequired(
                    canonical.MaterialName);
            var configuration =
                ConnectMutationCanonicalization.DecodeConfiguration(
                    encoded);
            var projected =
                ConnectMutationCanonicalization.ProjectConfiguration(
                    configuration);
            var fingerprint =
                ConnectMutationCanonicalization.ConfigurationFingerprint(
                    projected);

            if (!string.Equals(
                    fingerprint,
                    canonical.RequestedConfigurationFingerprint,
                    StringComparison.Ordinal))
            {
                return false;
            }

            mutation = new ConnectAlterMutation(
                cluster,
                connector,
                configuration);
            return true;
        }
        catch (Exception exception)
            when (exception is
                ArgumentException or
                KeyNotFoundException or
                MutationStateException or
                JsonException)
        {
            return false;
        }
    }

    private static bool TryBuildControlMutation(
        ConnectControlCanonicalIntent canonical,
        out ConnectControlMutation? mutation)
    {
        mutation = null;

        if (!Enum.IsDefined(canonical.Action) ||
            canonical.StateFingerprint.Length != 64 ||
            !canonical.StateFingerprint.All(char.IsAsciiHexDigit))
        {
            return false;
        }

        try
        {
            var cluster =
                ConnectMutationCanonicalization.RequireIdentifier(
                    canonical.ClusterId,
                    "Cluster ID",
                    256);
            var connector =
                ConnectMutationCanonicalization.RequireConnectorName(
                    canonical.ConnectorName);

            if (canonical.Action is
                    ConnectControlAction.Pause or
                    ConnectControlAction.Resume)
            {
                if (canonical.TaskId is not null ||
                    canonical.CurrentTaskState is not null)
                {
                    return false;
                }
            }
            else if (canonical.Action ==
                     ConnectControlAction.Restart)
            {
                if (canonical.TaskId is < 0)
                    return false;

                if (canonical.TaskId.HasValue &&
                    string.IsNullOrWhiteSpace(
                        canonical.CurrentTaskState))
                {
                    return false;
                }
            }

            mutation = new ConnectControlMutation(
                cluster,
                connector,
                canonical.TaskId,
                canonical.Action);
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool TryBuildDeleteMutation(
        ConnectDeleteCanonicalIntent canonical,
        out ConnectDeleteMutation? mutation)
    {
        mutation = null;

        if (canonical.StateFingerprint.Length != 64 ||
            !canonical.StateFingerprint.All(char.IsAsciiHexDigit) ||
            canonical.CurrentConfigurationFingerprint.Length != 64 ||
            !canonical.CurrentConfigurationFingerprint.All(
                char.IsAsciiHexDigit))
        {
            return false;
        }

        try
        {
            mutation = new ConnectDeleteMutation(
                ConnectMutationCanonicalization.RequireIdentifier(
                    canonical.ClusterId,
                    "Cluster ID",
                    256),
                ConnectMutationCanonicalization.RequireConnectorName(
                    canonical.ConnectorName));
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static bool ValidateRequestedConfiguration(
        string materialName,
        string fingerprint,
        IReadOnlyList<ConnectConfigurationCanonicalItem> items)
    {
        try
        {
            _ = ConnectMutationCanonicalization.RequireIdentifier(
                materialName,
                "Connect material name",
                256);
        }
        catch (ArgumentException)
        {
            return false;
        }

        if (fingerprint.Length != 64 ||
            !fingerprint.All(char.IsAsciiHexDigit) ||
            items is null ||
            items.Count is < 1 or >
                ConnectMutationPolicy.HardMaxConfigurationItems ||
            items.Any(item =>
                item is null ||
                item.ValueSha256.Length != 64 ||
                !item.ValueSha256.All(char.IsAsciiHexDigit)))
        {
            return false;
        }

        return string.Equals(
            ConnectMutationCanonicalization.ConfigurationFingerprint(
                items),
            fingerprint,
            StringComparison.Ordinal);
    }

    private static MutationProviderResult Verified(
        string code,
        MutationProviderResult accepted,
        ConnectMutationObservation? observation)
    {
        var evidence = MergeEvidence(
            accepted.SafeEvidence);
        evidence["verification.state"] = "observed";

        if (observation is not null)
        {
            evidence["connector.exists"] =
                observation.Exists ? "true" : "false";
            evidence["connector.state"] =
                observation.State;
            evidence["configuration.fingerprint"] =
                observation.ConfigurationFingerprint;
        }

        return new MutationProviderResult(
            MutationExecutionResultKind.AppliedVerified,
            code,
            evidence);
    }

    private static MutationProviderResult AppliedUnverified(
        string code,
        MutationProviderResult accepted,
        ConnectMutationObservation? observation)
    {
        var evidence = MergeEvidence(
            accepted.SafeEvidence);
        evidence["verification.state"] = "inconclusive";

        if (observation is not null)
        {
            evidence["connector.exists"] =
                observation.Exists ? "true" : "false";
            evidence["connector.state"] =
                observation.State;
            evidence["configuration.fingerprint"] =
                observation.ConfigurationFingerprint;
        }

        return new MutationProviderResult(
            MutationExecutionResultKind.AppliedUnverified,
            code,
            evidence);
    }

    private static MutationProviderResult Unknown(
        string code) =>
        new(
            MutationExecutionResultKind.ExecutionUnknown,
            code);

    private static Dictionary<string, string> MergeEvidence(
        IReadOnlyDictionary<string, string>? source)
    {
        var result = new Dictionary<string, string>(
            StringComparer.Ordinal);

        if (source is not null)
        {
            foreach (var pair in source)
                result[pair.Key] = pair.Value;
        }

        return result;
    }

    private sealed record ConnectVerificationObservation(
        bool Verified,
        ConnectMutationObservation? Observation);
}

public sealed class ConnectCreateExecutionHandler :
    IMutationExecutionHandler
{
    private readonly ConnectMutationExecutionService _service;

    public ConnectCreateExecutionHandler(
        ConnectMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.ConnectCreate;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default) =>
        _service.CreateAsync(
            context,
            cancellationToken);
}

public sealed class ConnectAlterExecutionHandler :
    IMutationExecutionHandler
{
    private readonly ConnectMutationExecutionService _service;

    public ConnectAlterExecutionHandler(
        ConnectMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.ConnectAlter;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != OperationKind)
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "connect_alter_operation_mismatch"));
        }

        if (!TryReadAlterKind(
                context.Operation.CanonicalIntent,
                out var kind))
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "connect_alter_canonical_invalid"));
        }

        switch (kind)
        {
            case ConnectAlterIntentKind.ConfigurationUpdate:
                return _service.UpdateAsync(
                    context,
                    cancellationToken);

            case ConnectAlterIntentKind.Control:
                ConnectControlCanonicalIntent canonical;
                try
                {
                    canonical =
                        ConnectMutationCanonicalization.Deserialize<
                            ConnectControlCanonicalIntent>(
                            context.Operation.CanonicalIntent);
                }
                catch
                {
                    return Task.FromResult(
                        new MutationProviderResult(
                            MutationExecutionResultKind.ExecutionUnknown,
                            "connect_control_canonical_invalid"));
                }

                return _service.ControlAsync(
                    canonical,
                    cancellationToken,
                    context.ExecutionDeadlineUtc);

            default:
                return Task.FromResult(
                    new MutationProviderResult(
                        MutationExecutionResultKind.ExecutionUnknown,
                        "connect_alter_canonical_invalid"));
        }
    }

    private static bool TryReadAlterKind(
        string canonicalIntent,
        out ConnectAlterIntentKind kind)
    {
        kind = default;

        try
        {
            using var document =
                JsonDocument.Parse(
                    canonicalIntent);
            if (document.RootElement.ValueKind !=
                    JsonValueKind.Object ||
                !document.RootElement.TryGetProperty(
                    "alterKind",
                    out var element) ||
                !element.TryGetInt32(out var raw) ||
                !Enum.IsDefined(
                    (ConnectAlterIntentKind)raw))
            {
                return false;
            }

            kind = (ConnectAlterIntentKind)raw;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}

public sealed class ConnectDeleteExecutionHandler :
    IMutationExecutionHandler
{
    private readonly ConnectMutationExecutionService _service;

    public ConnectDeleteExecutionHandler(
        ConnectMutationExecutionService service)
    {
        _service = service ?? throw new ArgumentNullException(nameof(service));
    }

    public MutationOperationKind OperationKind =>
        MutationOperationKind.ConnectDelete;

    public Task<MutationProviderResult> ExecuteAsync(
        MutationExecutionContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.Operation.OperationKind != OperationKind)
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "connect_delete_operation_mismatch"));
        }

        ConnectDeleteCanonicalIntent canonical;
        try
        {
            canonical =
                ConnectMutationCanonicalization.Deserialize<
                    ConnectDeleteCanonicalIntent>(
                    context.Operation.CanonicalIntent);
        }
        catch
        {
            return Task.FromResult(
                new MutationProviderResult(
                    MutationExecutionResultKind.ExecutionUnknown,
                    "connect_delete_canonical_invalid"));
        }

        return _service.DeleteAsync(
            canonical,
            cancellationToken,
            context.ExecutionDeadlineUtc);
    }
}
