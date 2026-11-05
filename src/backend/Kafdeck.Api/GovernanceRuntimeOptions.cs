using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

public sealed class GovernanceRuntimeOptions
{
    public bool CatalogEnabled { get; set; }
    public bool PoliciesEnabled { get; set; }
    public bool RequestsEnabled { get; set; }
    public bool AttributionEnabled { get; set; }
    public Guid? DeploymentGeneration { get; set; }
    public bool RestoreMaintenanceMode { get; set; }

    public void Validate(bool oidc, bool mutations, bool generationAdmission)
    {
        if (DeploymentGeneration == Guid.Empty || (CatalogEnabled || PoliciesEnabled || RequestsEnabled || AttributionEnabled) && (!oidc || !mutations || DeploymentGeneration is null))
            throw new KafdeckConfigurationException("Governance endpoints require OIDC, governed mutations and an explicit outside deployment generation.");
        if (RestoreMaintenanceMode != generationAdmission || generationAdmission && (!mutations || DeploymentGeneration is null))
            throw new KafdeckConfigurationException("Governance generation admission requires explicit quarantined maintenance mode and cannot start HTTP/workers.");
    }
}
