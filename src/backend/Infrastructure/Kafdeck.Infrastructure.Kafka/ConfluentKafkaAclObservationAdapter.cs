using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Configuration;
using Kafdeck.Modules.Administration;
using ProviderAclBinding = Confluent.Kafka.Admin.AclBinding;
using ProviderAclBindingFilter = Confluent.Kafka.Admin.AclBindingFilter;
using ProviderAclOperation = Confluent.Kafka.Admin.AclOperation;
using ProviderAclPermissionType = Confluent.Kafka.Admin.AclPermissionType;
using ProviderResourcePatternType = Confluent.Kafka.Admin.ResourcePatternType;
using ProviderResourceType = Confluent.Kafka.Admin.ResourceType;

namespace Kafdeck.Infrastructure.Kafka;

public sealed class ConfluentKafkaAclObservationAdapter :
    IAclObservationPort,
    IDisposable
{
    public const int DefaultMaxReturnedEntries = 250;
    public const int HardMaxReturnedEntries = 1_024;

    private readonly KafkaAdminClientRegistry _clients;
    private readonly TimeProvider _timeProvider;
    private readonly int _maxReturnedEntries;

    public ConfluentKafkaAclObservationAdapter(
        IReadOnlyList<ClusterProfile> clusterProfiles,
        SecretResolver secretResolver,
        TimeProvider? timeProvider = null,
        int maxReturnedEntries = DefaultMaxReturnedEntries)
    {
        if (maxReturnedEntries is < 1 or > HardMaxReturnedEntries)
        {
            throw new ArgumentOutOfRangeException(nameof(maxReturnedEntries));
        }

        _clients = new KafkaAdminClientRegistry(
            clusterProfiles ?? throw new ArgumentNullException(nameof(clusterProfiles)),
            secretResolver ?? throw new ArgumentNullException(nameof(secretResolver)));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _maxReturnedEntries = maxReturnedEntries;
    }

    public async Task<KafkaResult<IReadOnlyList<KafkaAclBinding>>> DescribeAsync(
        string clusterId,
        KafkaAclBindingFilter filter,
        KafkaOperationContext operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterId);
        ArgumentNullException.ThrowIfNull(filter);

        if (cancellationToken.IsCancellationRequested)
        {
            return Failed(KafkaFailureMapper.Cancelled());
        }

        var now = _timeProvider.GetUtcNow();
        if (operation.IsExpired(now))
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }

        if (!_clients.ContainsCluster(clusterId))
        {
            return Failed(KafkaFailureMapper.ClusterNotConfigured());
        }

        ProviderAclBindingFilter providerFilter;
        try
        {
            providerFilter = ConfluentKafkaAclMapper.ToProviderFilter(
                AclMutationPolicy.NormalizeFilter(filter));
        }
        catch (NotSupportedException exception)
        {
            return Failed(new KafkaFailure(
                KafkaFailureCategory.NotSupported,
                "acl_resource_type_unsupported",
                exception.Message,
                false));
        }
        catch (ArgumentException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }

        try
        {
            var remaining = operation.Remaining(_timeProvider.GetUtcNow());
            if (remaining <= TimeSpan.Zero)
            {
                return Failed(KafkaFailureMapper.DeadlineExceeded());
            }

            using var deadline =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(remaining);

            var client = _clients.GetClient(clusterId);
            DescribeAclsResult result;
            try
            {
                result = await client.DescribeAclsAsync(
                        providerFilter,
                        new DescribeAclsOptions { RequestTimeout = remaining })
                    .WaitAsync(deadline.Token)
                    .ConfigureAwait(false);
            }
            catch (DescribeAclsException exception)
                when (exception.Result.Error.IsError)
            {
                throw new KafkaException(exception.Result.Error);
            }

            if (result.AclBindings.Count > _maxReturnedEntries)
            {
                return Failed(new KafkaFailure(
                    KafkaFailureCategory.ProtocolError,
                    "acl_response_entry_limit_exceeded",
                    "Kafka returned more ACL entries than the configured W42 observation limit.",
                    false));
            }

            var converted = new List<KafkaAclBinding>(result.AclBindings.Count);
            foreach (var binding in result.AclBindings)
            {
                if (!ConfluentKafkaAclMapper.TryFromProvider(
                        binding,
                        out var convertedBinding))
                {
                    return Failed(new KafkaFailure(
                        KafkaFailureCategory.NotSupported,
                        "acl_binding_shape_unsupported",
                        "Kafka returned an ACL binding shape that the pinned W42 contract does not admit.",
                        false));
                }

                converted.Add(convertedBinding!);
            }

            var normalized = converted
                .Select(AclMutationPolicy.NormalizeBinding)
                .Distinct()
                .OrderBy(AclBindingIdentity.Canonical, StringComparer.Ordinal)
                .ToArray();
            return KafkaResult<IReadOnlyList<KafkaAclBinding>>.Success(
                Array.AsReadOnly(normalized),
                LiveObservation());
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            return Failed(KafkaFailureMapper.Cancelled());
        }
        catch (OperationCanceledException)
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (TimeoutException)
        {
            return Failed(KafkaFailureMapper.DeadlineExceeded());
        }
        catch (KafdeckConfigurationException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (KeyNotFoundException)
        {
            return Failed(KafkaFailureMapper.ClusterNotConfigured());
        }
        catch (KafkaException exception)
        {
            return Failed(KafkaFailureMapper.FromKafka(exception.Error));
        }
        catch (ArgumentException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (InvalidOperationException)
        {
            return Failed(KafkaFailureMapper.InvalidConfiguration());
        }
        catch (Exception)
        {
            return Failed(KafkaFailureMapper.Unknown());
        }
    }

    public void Dispose() => _clients.Dispose();

    private KafkaResult<IReadOnlyList<KafkaAclBinding>> Failed(
        KafkaFailure failure) =>
        KafkaResult<IReadOnlyList<KafkaAclBinding>>.Failed(
            failure,
            LiveObservation());

    private ObservationMetadata LiveObservation()
    {
        var now = _timeProvider.GetUtcNow();
        return new ObservationMetadata(now, now, now, ObservationSource.Live);
    }
}

internal static class ConfluentKafkaAclMapper
{
    public static ProviderAclBindingFilter ToProviderFilter(
        KafkaAclBindingFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);

        var resourceType = filter.ResourceType is null
            ? ProviderResourceType.Any
            : ToProviderResourceType(filter.ResourceType.Value);

        return new ProviderAclBindingFilter
        {
            PatternFilter = new ResourcePatternFilter
            {
                Type = resourceType,
                Name = filter.ResourceName,
                ResourcePatternType = filter.PatternMode switch
                {
                    KafkaAclFilterPatternMode.Any => ProviderResourcePatternType.Any,
                    KafkaAclFilterPatternMode.Literal => ProviderResourcePatternType.Literal,
                    KafkaAclFilterPatternMode.Prefixed => ProviderResourcePatternType.Prefixed,
                    KafkaAclFilterPatternMode.Match => ProviderResourcePatternType.Match,
                    _ => throw new NotSupportedException(
                        "ACL filter pattern mode is not supported by the pinned provider."),
                },
            },
            EntryFilter = new AccessControlEntryFilter
            {
                Principal = filter.Principal,
                Host = filter.Host,
                Operation = filter.Operation is null
                    ? ProviderAclOperation.Any
                    : ToProviderOperation(filter.Operation.Value),
                PermissionType = filter.PermissionType is null
                    ? ProviderAclPermissionType.Any
                    : ToProviderPermission(filter.PermissionType.Value),
            },
        };
    }

    public static ProviderAclBinding ToProviderBinding(
        KafkaAclBinding binding)
    {
        var normalized = AclMutationPolicy.NormalizeBinding(binding);
        return new ProviderAclBinding
        {
            Pattern = new ResourcePattern
            {
                Type = ToProviderResourceType(normalized.ResourceType),
                Name = normalized.ResourceName,
                ResourcePatternType = normalized.PatternType switch
                {
                    KafkaAclPatternType.Literal => ProviderResourcePatternType.Literal,
                    KafkaAclPatternType.Prefixed => ProviderResourcePatternType.Prefixed,
                    _ => throw new NotSupportedException(
                        "ACL resource pattern type is not supported by the pinned provider."),
                },
            },
            Entry = new AccessControlEntry
            {
                Principal = normalized.Principal,
                Host = normalized.Host,
                Operation = ToProviderOperation(normalized.Operation),
                PermissionType = ToProviderPermission(normalized.PermissionType),
            },
        };
    }

    public static bool TryFromProvider(
        ProviderAclBinding binding,
        out KafkaAclBinding? result)
    {
        ArgumentNullException.ThrowIfNull(binding);
        result = null;

        if (!TryFromProviderResourceType(binding.Pattern.Type, out var resourceType) ||
            !TryFromProviderPattern(binding.Pattern.ResourcePatternType, out var patternType) ||
            !TryFromProviderOperation(binding.Entry.Operation, out var operation) ||
            !TryFromProviderPermission(binding.Entry.PermissionType, out var permission))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(binding.Pattern.Name) ||
            string.IsNullOrWhiteSpace(binding.Entry.Principal) ||
            string.IsNullOrWhiteSpace(binding.Entry.Host))
        {
            return false;
        }

        result = new KafkaAclBinding(
            resourceType,
            binding.Pattern.Name,
            patternType,
            binding.Entry.Principal,
            binding.Entry.Host,
            operation,
            permission);
        return true;
    }

    private static ProviderResourceType ToProviderResourceType(
        KafkaAclResourceType resourceType) =>
        resourceType switch
        {
            KafkaAclResourceType.Topic => ProviderResourceType.Topic,
            KafkaAclResourceType.Group => ProviderResourceType.Group,
            KafkaAclResourceType.Cluster => ProviderResourceType.Broker,
            KafkaAclResourceType.TransactionalId =>
                throw new NotSupportedException(
                    "The pinned Confluent.Kafka ResourceType enum does not expose TransactionalId; that ACL subpath remains blocked."),
            _ => throw new NotSupportedException(
                "ACL resource type is not supported by the pinned provider."),
        };

    private static ProviderAclOperation ToProviderOperation(
        KafkaAclOperation operation) =>
        operation switch
        {
            KafkaAclOperation.All => ProviderAclOperation.All,
            KafkaAclOperation.Read => ProviderAclOperation.Read,
            KafkaAclOperation.Write => ProviderAclOperation.Write,
            KafkaAclOperation.Create => ProviderAclOperation.Create,
            KafkaAclOperation.Delete => ProviderAclOperation.Delete,
            KafkaAclOperation.Alter => ProviderAclOperation.Alter,
            KafkaAclOperation.Describe => ProviderAclOperation.Describe,
            KafkaAclOperation.ClusterAction => ProviderAclOperation.ClusterAction,
            KafkaAclOperation.DescribeConfigs => ProviderAclOperation.DescribeConfigs,
            KafkaAclOperation.AlterConfigs => ProviderAclOperation.AlterConfigs,
            KafkaAclOperation.IdempotentWrite => ProviderAclOperation.IdempotentWrite,
            _ => throw new NotSupportedException(
                "ACL operation is not supported by the pinned provider."),
        };

    private static ProviderAclPermissionType ToProviderPermission(
        KafkaAclPermissionType permission) =>
        permission switch
        {
            KafkaAclPermissionType.Deny => ProviderAclPermissionType.Deny,
            KafkaAclPermissionType.Allow => ProviderAclPermissionType.Allow,
            _ => throw new NotSupportedException(
                "ACL permission type is not supported by the pinned provider."),
        };

    private static bool TryFromProviderResourceType(
        ProviderResourceType resourceType,
        out KafkaAclResourceType result)
    {
        result = resourceType switch
        {
            ProviderResourceType.Topic => KafkaAclResourceType.Topic,
            ProviderResourceType.Group => KafkaAclResourceType.Group,
            ProviderResourceType.Broker => KafkaAclResourceType.Cluster,
            _ => default,
        };
        return resourceType is
            ProviderResourceType.Topic or
            ProviderResourceType.Group or
            ProviderResourceType.Broker;
    }

    private static bool TryFromProviderPattern(
        ProviderResourcePatternType pattern,
        out KafkaAclPatternType result)
    {
        result = pattern switch
        {
            ProviderResourcePatternType.Literal => KafkaAclPatternType.Literal,
            ProviderResourcePatternType.Prefixed => KafkaAclPatternType.Prefixed,
            _ => default,
        };
        return pattern is
            ProviderResourcePatternType.Literal or
            ProviderResourcePatternType.Prefixed;
    }

    private static bool TryFromProviderOperation(
        ProviderAclOperation operation,
        out KafkaAclOperation result)
    {
        result = operation switch
        {
            ProviderAclOperation.All => KafkaAclOperation.All,
            ProviderAclOperation.Read => KafkaAclOperation.Read,
            ProviderAclOperation.Write => KafkaAclOperation.Write,
            ProviderAclOperation.Create => KafkaAclOperation.Create,
            ProviderAclOperation.Delete => KafkaAclOperation.Delete,
            ProviderAclOperation.Alter => KafkaAclOperation.Alter,
            ProviderAclOperation.Describe => KafkaAclOperation.Describe,
            ProviderAclOperation.ClusterAction => KafkaAclOperation.ClusterAction,
            ProviderAclOperation.DescribeConfigs => KafkaAclOperation.DescribeConfigs,
            ProviderAclOperation.AlterConfigs => KafkaAclOperation.AlterConfigs,
            ProviderAclOperation.IdempotentWrite => KafkaAclOperation.IdempotentWrite,
            _ => default,
        };
        return operation is
            ProviderAclOperation.All or
            ProviderAclOperation.Read or
            ProviderAclOperation.Write or
            ProviderAclOperation.Create or
            ProviderAclOperation.Delete or
            ProviderAclOperation.Alter or
            ProviderAclOperation.Describe or
            ProviderAclOperation.ClusterAction or
            ProviderAclOperation.DescribeConfigs or
            ProviderAclOperation.AlterConfigs or
            ProviderAclOperation.IdempotentWrite;
    }

    private static bool TryFromProviderPermission(
        ProviderAclPermissionType permission,
        out KafkaAclPermissionType result)
    {
        result = permission switch
        {
            ProviderAclPermissionType.Deny => KafkaAclPermissionType.Deny,
            ProviderAclPermissionType.Allow => KafkaAclPermissionType.Allow,
            _ => default,
        };
        return permission is
            ProviderAclPermissionType.Deny or
            ProviderAclPermissionType.Allow;
    }
}
