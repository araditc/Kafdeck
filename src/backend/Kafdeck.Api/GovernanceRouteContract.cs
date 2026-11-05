using Kafdeck.Core.Security;

namespace Kafdeck.Api;

/// <summary>Compiled named governance routes; registration never replaces current scoped RBAC.</summary>
internal sealed record GovernanceRouteContract(
    AuthorizationAction Action,
    bool Read,
    Type? RequestType = null,
    int MaxBodyBytes = 256 * 1024,
    string QueryKeys = "clusterId,pageSize,cursor",
    Type? ResponseType = null);
