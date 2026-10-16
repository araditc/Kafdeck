using Kafdeck.Contracts;
using Kafdeck.Core.Security;

namespace Kafdeck.Api;

internal static class BearerReadBoundary
{
    public static Type? RequestType(NamedReadContract contract) => contract.Body switch
    {
        null => null,
        "SchemaMockRequest" => typeof(SchemaMockRequest),
        "ControlledSerdeDecodeApiRequest" => typeof(ControlledSerdeDecodeApiRequest),
        "ControlledSerdeEncodeApiRequest" => typeof(ControlledSerdeEncodeApiRequest),
        "ConnectPluginValidationRequest" => typeof(KafdeckReadViewEndpoints.ConnectPluginValidationRequest),
        "KsqlQueryRequestBody" => typeof(KsqlQueryRequestBody),
        _ => throw new InvalidOperationException("Unknown compiled read request contract."),
    };

    public static bool Matches(HttpContext context, NamedReadContract? contract) => contract is not null &&
        string.Equals(context.Request.Method, contract.Method, StringComparison.OrdinalIgnoreCase);

    public static bool ValidQuery(IQueryCollection query, NamedReadContract contract)
    {
        var allowed = contract.QueryKeys.Split(',', StringSplitOptions.RemoveEmptyEntries).ToHashSet(StringComparer.Ordinal);
        return query.Count <= 32 && query.All(pair => allowed.Contains(pair.Key) && pair.Value.Count == 1 &&
            pair.Value[0] is { Length: <= 4096 } value && !value.Any(char.IsControl));
    }

    public static bool Registered(NonBrowserReadRegistry? registry, NonBrowserReadIdentity identity, NamedReadContract contract) =>
        (!contract.HumanOnly || identity.Kind == NonBrowserPrincipalKind.Human) &&
        (contract.Action == nameof(AuthorizationAction.NotificationManage)
            ? registry?.IsGovernedMutationAllowed(identity, AuthorizationAction.NotificationManage) == true
            : registry?.IsRegisteredReadAllowed(identity, Enum.Parse<AuthorizationAction>(contract.Action)) == true);

    public static KafdeckAuthorizationOutcome Authorize(HttpContext context, NamedReadContract contract)
    {
        var action = Enum.Parse<AuthorizationAction>(contract.Action);
        var authorization = context.RequestServices.GetRequiredService<KafdeckAuthorizationService>();
        var cluster = context.Request.RouteValues["clusterId"]?.ToString();
        if (contract.FilteredCollection) return authorization.AuthorizeCollection(context.User, action, cluster);
        var resource = new[] { "topicName", "subject", "groupId", "connectorName", "applicationId", "destinationId", "subscriptionId", "policyId", "brokerId" }
            .Select(key => context.Request.RouteValues[key]?.ToString()).FirstOrDefault(value => value is not null);
        return authorization.Authorize(context.User, new(action, cluster, resource));
    }
}
