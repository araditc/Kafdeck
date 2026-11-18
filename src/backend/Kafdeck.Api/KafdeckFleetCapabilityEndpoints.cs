namespace Kafdeck.Api;

public static class KafdeckFleetCapabilityEndpoints
{
    public static WebApplication MapKafdeckFleetCapabilities(
        this WebApplication app, bool brokerConfigEnabled = false)
    {
        ArgumentNullException.ThrowIfNull(app);

        app.MapGet(
                "/api/v1/fleet/capabilities",
                () => Results.Ok(new
                {
                    version = "v0.6",
                    capabilities = FleetCapabilityCatalog.All.Select(item =>
                        item.Id != "dynamic-configuration" || brokerConfigEnabled ? item : item with
                        {
                            State = FleetCapabilityState.Unconfigured,
                            Reason = "Enable the governed mutation runtime to manage dynamic broker settings.",
                        }),
                }))
            .WithName("v06-fleet-capabilities");

        return app;
    }
}
