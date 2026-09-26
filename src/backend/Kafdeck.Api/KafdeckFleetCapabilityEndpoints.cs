namespace Kafdeck.Api;

public static class KafdeckFleetCapabilityEndpoints
{
    public static WebApplication MapKafdeckFleetCapabilities(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet(
                "/api/v1/fleet/capabilities",
                () => Results.Ok(new
                {
                    version = "v0.6",
                    capabilities = FleetCapabilityCatalog.All,
                }))
            .WithName("v06-fleet-capabilities");

        return app;
    }
}
