using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Core.Security;

namespace Kafdeck.Modules.Administration;

public enum ScramMutationMode
{
    Upsert = 1,
    Delete = 2,
}

public enum ScramMutationPlanningFailureCode
{
    InvalidInput = 1,
    ProtectedUser = 2,
    PolicyDenied = 3,
    CredentialNotFound = 4,
    ProviderUnauthorized = 5,
    ProviderUnsupported = 6,
    ProviderUnavailable = 7,
    ObservationFailed = 8,
}

public sealed record ScramMutationPlanningFailure(
    ScramMutationPlanningFailureCode Code,
    string SafeMessage);

public sealed record ScramMutationPlan(
    ScramMutationMode Mode,
    ScramCredentialBindingDescriptor Credential,
    string ObservedMetadataFingerprint,
    string? MaterialName);

public sealed record ScramMutationPlanningResult(
    ScramMutationPlan? Plan,
    MutationIntentDescriptor? Intent,
    MutationRiskDecision? Risk,
    ScramMutationPlanningFailure? Failure)
{
    public bool IsSuccess =>
        Plan is not null &&
        Intent is not null &&
        Risk is not null &&
        Failure is null;

    public static ScramMutationPlanningResult Success(
        ScramMutationPlan plan,
        MutationIntentDescriptor intent,
        MutationRiskDecision risk) =>
        new(plan, intent, risk, null);

    public static ScramMutationPlanningResult Failed(
        ScramMutationPlanningFailure failure) =>
        new(null, null, null, failure);
}

public sealed class ScramServerPolicy
{
    private readonly HashSet<string> _protectedUsers;
    private readonly HashSet<KafkaScramMechanism> _allowedMechanisms;

    public ScramServerPolicy(
        IEnumerable<string> protectedUsers,
        IEnumerable<KafkaScramMechanism> allowedMechanisms,
        int minIterations = 4096,
        int maxIterations = 1_000_000)
    {
        ArgumentNullException.ThrowIfNull(protectedUsers);
        ArgumentNullException.ThrowIfNull(allowedMechanisms);
        if (minIterations < 1 || maxIterations < minIterations)
        {
            throw new ArgumentOutOfRangeException(nameof(minIterations));
        }

        _protectedUsers = protectedUsers
            .Select(ScramCredentialPolicy.NormalizeUser)
            .ToHashSet(StringComparer.Ordinal);
        _allowedMechanisms = allowedMechanisms
            .Select(mechanism =>
            {
                if (!Enum.IsDefined(mechanism))
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(allowedMechanisms));
                }

                return mechanism;
            })
            .ToHashSet();

        if (_allowedMechanisms.Count == 0)
        {
            throw new ArgumentException(
                "At least one SCRAM mechanism must be admitted.",
                nameof(allowedMechanisms));
        }

        MinIterations = minIterations;
        MaxIterations = maxIterations;
    }

    public int MinIterations { get; }
    public int MaxIterations { get; }

    public void ValidateMutation(
        string user,
        KafkaScramMechanism mechanism,
        int iterations)
    {
        var normalizedUser = ScramCredentialPolicy.NormalizeUser(user);
        if (_protectedUsers.Contains(normalizedUser))
        {
            throw new ScramPolicyException(
                ScramMutationPlanningFailureCode.ProtectedUser,
                "SCRAM mutation targets a server-protected user.");
        }

        if (!_allowedMechanisms.Contains(mechanism) ||
            iterations < MinIterations ||
            iterations > MaxIterations)
        {
            throw new ScramPolicyException(
                ScramMutationPlanningFailureCode.PolicyDenied,
                "SCRAM mutation exceeds the server-owned mechanism or iteration policy.");
        }
    }

    public void ValidateDelete(
        string user,
        KafkaScramMechanism mechanism)
    {
        var normalizedUser = ScramCredentialPolicy.NormalizeUser(user);
        if (_protectedUsers.Contains(normalizedUser))
        {
            throw new ScramPolicyException(
                ScramMutationPlanningFailureCode.ProtectedUser,
                "SCRAM mutation targets a server-protected user.");
        }

        if (!_allowedMechanisms.Contains(mechanism))
        {
            throw new ScramPolicyException(
                ScramMutationPlanningFailureCode.PolicyDenied,
                "SCRAM mutation exceeds the server-owned mechanism policy.");
        }
    }
}

public sealed class ScramPolicyException : ArgumentException
{
    public ScramPolicyException(
        ScramMutationPlanningFailureCode code,
        string message)
        : base(message)
    {
        Code = code;
    }

    public ScramMutationPlanningFailureCode Code { get; }
}

public sealed record ScramMutationPlannerPolicy(TimeSpan ObservationTimeout)
{
    public static ScramMutationPlannerPolicy Default { get; } =
        new(TimeSpan.FromSeconds(10));
}

public sealed class ScramMutationPlanner
{
    public const string MaterialName = "scram/credential";

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    private readonly IScramObservationPort _observations;
    private readonly IMutationMaterialDigestService _digest;
    private readonly ScramServerPolicy _serverPolicy;
    private readonly ScramMutationPlannerPolicy _plannerPolicy;
    private readonly TimeProvider _timeProvider;

    public ScramMutationPlanner(
        IScramObservationPort observations,
        IMutationMaterialDigestService digest,
        ScramServerPolicy serverPolicy,
        ScramMutationPlannerPolicy? plannerPolicy = null,
        TimeProvider? timeProvider = null)
    {
        _observations = observations ??
                        throw new ArgumentNullException(nameof(observations));
        _digest = digest ?? throw new ArgumentNullException(nameof(digest));
        _serverPolicy = serverPolicy ??
                        throw new ArgumentNullException(nameof(serverPolicy));
        _plannerPolicy = plannerPolicy ?? ScramMutationPlannerPolicy.Default;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_plannerPolicy.ObservationTimeout < TimeSpan.FromSeconds(1) ||
            _plannerPolicy.ObservationTimeout > TimeSpan.FromSeconds(30))
        {
            throw new ArgumentOutOfRangeException(nameof(plannerPolicy));
        }
    }

    public async Task<ScramMutationPlanningResult> PlanUpsertAsync(
        string clusterId,
        string user,
        KafkaScramMechanism mechanism,
        int iterations,
        ReadOnlyMemory<byte> password,
        CancellationToken cancellationToken = default)
    {
        byte[]? envelope = null;
        try
        {
            var descriptor = NormalizeDescriptor(
                clusterId,
                user,
                mechanism,
                iterations);
            _serverPolicy.ValidateMutation(
                descriptor.User,
                descriptor.Mechanism,
                descriptor.Iterations);

            var observed = await ObserveAsync(
                    descriptor.ClusterId,
                    descriptor.User,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!observed.IsSuccess || observed.Value is null)
            {
                return Failed(observed.Failure);
            }

            envelope = ScramCredentialMaterialCodec.Encode(
                descriptor,
                password.Span);
            var digest = _digest.ComputeDigest(envelope);
            var plan = new ScramMutationPlan(
                ScramMutationMode.Upsert,
                descriptor,
                FingerprintMetadata(descriptor.User, observed.Value),
                MaterialName);
            return Success(
                plan,
                new[]
                {
                    new MutationMaterialDigest(MaterialName, digest),
                });
        }
        catch (ScramPolicyException exception)
        {
            return Failed(exception.Code, exception.Message);
        }
        catch (ArgumentException)
        {
            return Failed(
                ScramMutationPlanningFailureCode.InvalidInput,
                "SCRAM upsert request is invalid.");
        }
        finally
        {
            if (envelope is not null)
            {
                CryptographicOperations.ZeroMemory(envelope);
            }
        }
    }

    public async Task<ScramMutationPlanningResult> PlanDeleteAsync(
        string clusterId,
        string user,
        KafkaScramMechanism mechanism,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var normalizedCluster = NormalizeCluster(clusterId);
            var normalizedUser = ScramCredentialPolicy.NormalizeUser(user);
            if (!Enum.IsDefined(mechanism))
            {
                throw new ArgumentOutOfRangeException(nameof(mechanism));
            }

            _serverPolicy.ValidateDelete(normalizedUser, mechanism);
            var observed = await ObserveAsync(
                    normalizedCluster,
                    normalizedUser,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!observed.IsSuccess || observed.Value is null)
            {
                return Failed(observed.Failure);
            }

            var current = observed.Value.SingleOrDefault(
                item => item.Mechanism == mechanism);
            if (current is null)
            {
                return Failed(
                    ScramMutationPlanningFailureCode.CredentialNotFound,
                    "The requested SCRAM mechanism is not currently observed for the exact user.");
            }

            var descriptor = new ScramCredentialBindingDescriptor(
                normalizedCluster,
                normalizedUser,
                mechanism,
                current.Iterations);
            var plan = new ScramMutationPlan(
                ScramMutationMode.Delete,
                descriptor,
                FingerprintMetadata(normalizedUser, observed.Value),
                MaterialName: null);
            return Success(plan, Array.Empty<MutationMaterialDigest>());
        }
        catch (ScramPolicyException exception)
        {
            return Failed(exception.Code, exception.Message);
        }
        catch (ArgumentException)
        {
            return Failed(
                ScramMutationPlanningFailureCode.InvalidInput,
                "SCRAM delete request is invalid.");
        }
    }

    public static string FingerprintMetadata(
        string user,
        IEnumerable<KafkaScramCredentialMetadata> metadata)
    {
        var normalized = ScramCredentialPolicy.NormalizeMetadataSet(
            user,
            metadata);
        var builder = new StringBuilder();
        foreach (var item in normalized)
        {
            builder
                .Append((int)item.Mechanism)
                .Append(':')
                .Append(item.Iterations)
                .Append('\n');
        }

        return Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString())))
            .ToLowerInvariant();
    }

    private async Task<KafkaResult<IReadOnlyList<KafkaScramCredentialMetadata>>>
        ObserveAsync(
            string clusterId,
            string user,
            CancellationToken cancellationToken)
    {
        return await _observations.DescribeUserAsync(
                clusterId,
                user,
                new KafkaOperationContext(
                    _timeProvider.GetUtcNow().Add(
                        _plannerPolicy.ObservationTimeout)),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private ScramMutationPlanningResult Success(
        ScramMutationPlan plan,
        IReadOnlyList<MutationMaterialDigest> materialDigests)
    {
        var conflictResource = FleetConflictKeyCodec.Encode(
            new FleetConflictTarget(
                FleetConflictTargetKind.ScramCredential,
                plan.Credential.ClusterId,
                plan.Credential.User,
                ((int)plan.Credential.Mechanism).ToString(
                    System.Globalization.CultureInfo.InvariantCulture)));

        var requirements = FleetMutationAuthorization.NormalizeRequirements(
            MutationOperationKind.ScramAlter,
            new[]
            {
                new MutationAuthorizationTarget(
                    AuthorizationAction.ScramRead,
                    plan.Credential.ClusterId,
                    conflictResource),
                new MutationAuthorizationTarget(
                    AuthorizationAction.ScramAlter,
                    plan.Credential.ClusterId,
                    conflictResource),
            });

        var canonical = JsonSerializer.Serialize(plan, CanonicalJson);
        if (canonical.Length > MutationLimits.MaxCanonicalIntentCharacters)
        {
            return Failed(
                ScramMutationPlanningFailureCode.InvalidInput,
                "SCRAM safe canonical intent exceeds the admitted bound.");
        }

        var intent = new MutationIntentDescriptor(
            MutationOperationKind.ScramAlter,
            plan.Credential.ClusterId,
            canonical,
            new[] { conflictResource },
            new[]
            {
                new MutationPrecondition(
                    "scram.metadata",
                    plan.ObservedMetadataFingerprint),
            },
            materialDigests,
            requirements);

        return ScramMutationPlanningResult.Success(
            plan,
            intent,
            CriticalRisk());
    }

    private static MutationRiskDecision CriticalRisk() =>
        new(
            MutationRiskClass.Critical,
            Array.AsReadOnly(
                new[]
                {
                    "operation_floor:critical",
                    "scram_credential_mutation",
                }),
            MutationConfirmationMode.TypedTarget,
            RequiresIndependentApproval: true);

    private static ScramCredentialBindingDescriptor NormalizeDescriptor(
        string clusterId,
        string user,
        KafkaScramMechanism mechanism,
        int iterations) =>
        ScramCredentialMaterialBinding.Normalize(
            new ScramCredentialBindingDescriptor(
                NormalizeCluster(clusterId),
                ScramCredentialPolicy.NormalizeUser(user),
                mechanism,
                iterations));

    private static string NormalizeCluster(string clusterId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        if (clusterId.Length > 256 ||
            clusterId.Any(char.IsControl) ||
            !string.Equals(clusterId, clusterId.Trim(), StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "SCRAM physical cluster ID is invalid.",
                nameof(clusterId));
        }

        return clusterId;
    }

    private static ScramMutationPlanningResult Failed(
        ScramMutationPlanningFailureCode code,
        string safeMessage) =>
        ScramMutationPlanningResult.Failed(
            new ScramMutationPlanningFailure(code, safeMessage));

    private static ScramMutationPlanningResult Failed(KafkaFailure? failure) =>
        failure?.Category switch
        {
            KafkaFailureCategory.Unauthorized =>
                Failed(
                    ScramMutationPlanningFailureCode.ProviderUnauthorized,
                    "Kafka denied SCRAM metadata observation required for safe planning."),
            KafkaFailureCategory.NotSupported =>
                Failed(
                    ScramMutationPlanningFailureCode.ProviderUnsupported,
                    "Kafka or the pinned client does not support the required SCRAM metadata operation."),
            KafkaFailureCategory.Unavailable or
            KafkaFailureCategory.Timeout or
            KafkaFailureCategory.AuthenticationFailed or
            KafkaFailureCategory.TlsFailure =>
                Failed(
                    ScramMutationPlanningFailureCode.ProviderUnavailable,
                    "Kafka SCRAM metadata is not currently observable for safe planning."),
            _ =>
                Failed(
                    ScramMutationPlanningFailureCode.ObservationFailed,
                    "Kafka SCRAM metadata could not be safely observed."),
        };
}
