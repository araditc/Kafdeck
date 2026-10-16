using Kafdeck.Core.Security;
using Kafdeck.Modules.Administration;

namespace Kafdeck.Api;

internal static class BearerPlanningPreflight
{
    public static IResult? Authorize(EndpointFilterInvocationContext invocation)
    {
        var targets = new List<MutationAuthorizationTarget>();
        if (invocation.Arguments.OfType<DataJobPreviewRequest>().SingleOrDefault() is { } job)
        {
            if (job.Ranges is not { Count: > 0 and <= 100 }) return Invalid();
            targets.Add(new(AuthorizationAction.ClusterRead, job.SourceClusterId, job.SourceClusterId));
            targets.Add(new(AuthorizationAction.ClusterRead, job.DestinationClusterId, job.DestinationClusterId));
            foreach (var range in job.Ranges)
            {
                if (range is null) return Invalid();
                targets.Add(new(AuthorizationAction.TopicRead, job.SourceClusterId, range.SourceTopic));
                targets.Add(new(AuthorizationAction.RecordRead, job.SourceClusterId, range.SourceTopic));
                targets.Add(new(AuthorizationAction.RecordExport, job.SourceClusterId, range.SourceTopic));
                targets.Add(new(AuthorizationAction.TopicRead, job.DestinationClusterId, range.DestinationTopic));
                targets.Add(new(AuthorizationAction.RecordProduce, job.DestinationClusterId, range.DestinationTopic));
            }
        }
        if (invocation.Arguments.OfType<DataGeneratorPreviewRequest>().SingleOrDefault() is { } generator)
        {
            targets.Add(new(AuthorizationAction.ClusterRead, generator.DestinationClusterId, generator.DestinationClusterId));
            targets.Add(new(AuthorizationAction.TopicRead, generator.DestinationClusterId, generator.DestinationTopic));
            targets.Add(new(AuthorizationAction.RecordProduce, generator.DestinationClusterId, generator.DestinationTopic));
            if (generator.Source is not null && string.Equals(generator.Source.Kind, "schema", StringComparison.OrdinalIgnoreCase))
                targets.Add(new(AuthorizationAction.SchemaRead, generator.DestinationClusterId, generator.Source.SchemaSubject!));
        }
        if (targets.Count == 0) return null;
        if (targets.Any(t => string.IsNullOrWhiteSpace(t.ClusterId) || string.IsNullOrWhiteSpace(t.ResourceName))) return Invalid();
        var context = invocation.HttpContext;
        return context.RequestServices.GetRequiredService<MutationRequestAuthorizationService>().AuthorizeTargets(context.User, targets)
            == KafdeckAuthorizationOutcome.Allowed ? null : Results.Problem(statusCode: 403, title: "Current source/destination permissions are required before observation.");
    }
    private static IResult Invalid() => Results.Problem(statusCode: 400, title: "Invalid bounded source/destination request.");
}
