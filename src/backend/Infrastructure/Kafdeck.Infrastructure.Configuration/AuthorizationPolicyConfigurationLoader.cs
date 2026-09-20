using Kafdeck.Core.Security;
using Microsoft.Extensions.Configuration;

namespace Kafdeck.Infrastructure.Configuration;

public static class AuthorizationPolicyConfigurationLoader
{
    public static AuthorizationPolicyDefinition Load(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var section = configuration.GetSection("Kafdeck:Authorization");
        var roles = section
            .GetSection("Roles")
            .GetChildren()
            .Select(LoadRole)
            .ToArray();

        var subjectBindings = section
            .GetSection("SubjectBindings")
            .GetChildren()
            .Select(LoadSubjectBinding)
            .ToArray();

        var groupBindings = section
            .GetSection("GroupBindings")
            .GetChildren()
            .Select(LoadGroupBinding)
            .ToArray();

        return new AuthorizationPolicyDefinition(
            Array.AsReadOnly(roles),
            Array.AsReadOnly(subjectBindings),
            Array.AsReadOnly(groupBindings));
    }

    private static AuthorizationRoleDefinition LoadRole(IConfigurationSection section)
    {
        var permissions = section
            .GetSection("Permissions")
            .GetChildren()
            .Select(LoadPermission)
            .ToArray();

        return new AuthorizationRoleDefinition(
            section["Id"] ?? string.Empty,
            Array.AsReadOnly(permissions));
    }

    private static AuthorizationPermissionDefinition LoadPermission(IConfigurationSection section)
    {
        var action = ParseAction(section["Action"]);
        var clusterIds = ReadValues(section.GetSection("ClusterIds"));
        var resourcePatterns = ReadValues(section.GetSection("ResourcePatterns"));

        return new AuthorizationPermissionDefinition(
            action,
            Array.AsReadOnly(clusterIds),
            Array.AsReadOnly(resourcePatterns));
    }

    private static AuthorizationSubjectBindingDefinition LoadSubjectBinding(IConfigurationSection section) =>
        new(
            section["Issuer"] ?? string.Empty,
            section["Subject"] ?? string.Empty,
            Array.AsReadOnly(ReadValues(section.GetSection("RoleIds"))));

    private static AuthorizationGroupBindingDefinition LoadGroupBinding(IConfigurationSection section) =>
        new(
            section["ExternalGroup"] ?? string.Empty,
            Array.AsReadOnly(ReadValues(section.GetSection("RoleIds"))));

    private static string[] ReadValues(IConfigurationSection section) =>
        section.GetChildren()
            .Select(child => child.Value ?? string.Empty)
            .ToArray();

    private static AuthorizationAction ParseAction(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new KafdeckConfigurationException("Authorization permission action is required.");
        }

        var normalized = value
            .Replace("-", string.Empty, StringComparison.Ordinal)
            .Replace("_", string.Empty, StringComparison.Ordinal)
            .Replace(".", string.Empty, StringComparison.Ordinal)
            .Trim();

        foreach (var action in Enum.GetValues<AuthorizationAction>())
        {
            if (string.Equals(
                    action.ToString(),
                    normalized,
                    StringComparison.OrdinalIgnoreCase))
            {
                return action;
            }
        }

        var aliases = new Dictionary<string, AuthorizationAction>(StringComparer.OrdinalIgnoreCase)
        {
            ["systemread"] = AuthorizationAction.SystemRead,
            ["clusterread"] = AuthorizationAction.ClusterRead,
            ["brokerread"] = AuthorizationAction.BrokerRead,
            ["topiclist"] = AuthorizationAction.TopicList,
            ["topicread"] = AuthorizationAction.TopicRead,
            ["topicconfigread"] = AuthorizationAction.TopicConfigRead,
            ["brokerconfigread"] = AuthorizationAction.BrokerConfigRead,
        };

        if (aliases.TryGetValue(normalized, out var mapped))
        {
            return mapped;
        }

        throw new KafdeckConfigurationException($"Authorization action '{value}' is unsupported.");
    }
}
