using Kafdeck.Core.Security;

namespace Kafdeck.Api;

// Admission metadata belongs only to named opt-in backend routes.
internal sealed record DeclarativeRouteContract(AuthorizationAction Action, Type? RequestType = null);
