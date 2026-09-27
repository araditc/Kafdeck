using System.Text.Json;
using Kafdeck.Core.Records;
using Kafdeck.Core.Security;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

public sealed record ControlledSerdeDecodeApiRequest(
    ControlledSerdeFormat Format,
    string PayloadBase64);

public sealed record ControlledSerdeEncodeApiRequest(
    ControlledSerdeFormat Format,
    JsonElement StructuredValue);

public sealed record ControlledSerdeEncodedApiData(
    ControlledSerdeFormat Format,
    string PayloadBase64,
    int ByteCount);

public sealed record ControlledSerdeCapabilitiesData(
    ControlledSerdeLimitsData Limits,
    IReadOnlyList<ControlledSerdeCapability> Formats);

public sealed record ControlledSerdeLimitsData(
    int MaxInputBytes,
    int MaxOutputBytes,
    int MaxDepth,
    int MaxNodes,
    int MaxCollectionItems,
    int MaxStringCharacters,
    int MaxBinaryBytes);

public static class KafdeckControlledSerdeEndpoints
{
    private static readonly TimeSpan OperationTimeout =
        TimeSpan.FromSeconds(10);

    public static WebApplication MapKafdeckV07ControlledSerde(
        this WebApplication app,
        KafdeckOptions options)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(options);

        app.MapGet(
                "/api/v1/tools/serde/capabilities",
                (IControlledSerdePort serde) =>
                {
                    var limits = ControlledSerdeLimits.Default;
                    return Results.Ok(
                        new ControlledSerdeCapabilitiesData(
                            new ControlledSerdeLimitsData(
                                limits.MaxInputBytes,
                                limits.MaxOutputBytes,
                                limits.MaxDepth,
                                limits.MaxNodes,
                                limits.MaxCollectionItems,
                                limits.MaxStringCharacters,
                                limits.MaxBinaryBytes),
                            serde.GetCapabilities()));
                })
            .WithName("v07-serde-capabilities")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SystemRead);

        var decode = app.MapPost(
                "/api/v1/tools/serde/decode",
                async (
                    ControlledSerdeDecodeApiRequest request,
                    IControlledSerdePort serde,
                    CancellationToken cancellationToken) =>
                {
                    if (string.IsNullOrWhiteSpace(
                            request.PayloadBase64))
                    {
                        return Invalid(
                            "A non-empty base64 payload is required.");
                    }

                    byte[] payload;
                    try
                    {
                        payload = Convert.FromBase64String(
                            request.PayloadBase64);
                    }
                    catch (FormatException)
                    {
                        return Invalid(
                            "PayloadBase64 must be valid base64.");
                    }

                    if (payload.Length >
                        ControlledSerdeLimits.Default.MaxInputBytes)
                    {
                        return Results.Problem(
                            statusCode:
                                StatusCodes.Status422UnprocessableEntity,
                            type:
                                "urn:kafdeck:problem:serde-bound-exceeded",
                            title:
                                "SerDe input bound exceeded",
                            detail:
                                "Decoded payload exceeds the configured server bound.");
                    }

                    var result = await serde
                        .DecodeAsync(
                            new ControlledSerdeDecodeRequest(
                                request.Format,
                                payload),
                            ControlledSerdeLimits.Default,
                            DateTimeOffset.UtcNow.Add(
                                OperationTimeout),
                            cancellationToken)
                        .ConfigureAwait(false);

                    return Result(result);
                })
            .WithName("v07-serde-decode")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SystemRead);

        var encode = app.MapPost(
                "/api/v1/tools/serde/encode",
                async (
                    ControlledSerdeEncodeApiRequest request,
                    IControlledSerdePort serde,
                    CancellationToken cancellationToken) =>
                {
                    var result = await serde
                        .EncodeAsync(
                            new ControlledSerdeEncodeRequest(
                                request.Format,
                                request.StructuredValue),
                            ControlledSerdeLimits.Default,
                            DateTimeOffset.UtcNow.Add(
                                OperationTimeout),
                            cancellationToken)
                        .ConfigureAwait(false);

                    if (!result.IsSuccess ||
                        result.Value is null)
                    {
                        return Problem(result.Failure!);
                    }

                    var bytes =
                        result.Value.Payload.ToArray();
                    return Results.Ok(
                        new ControlledSerdeEncodedApiData(
                            result.Value.Format,
                            Convert.ToBase64String(bytes),
                            bytes.Length));
                })
            .WithName("v07-serde-encode")
            .RequireKafdeckAuthorization(
                AuthorizationAction.SystemRead);

        if (options.Deployment.Mode == AccessMode.Oidc)
        {
            decode.RequireKafdeckAntiforgery();
            encode.RequireKafdeckAntiforgery();
        }

        return app;
    }

    private static IResult Result(
        ControlledSerdeResult<ControlledSerdeDecodedValue> result)
    {
        if (!result.IsSuccess ||
            result.Value is null)
        {
            return Problem(result.Failure!);
        }

        return Results.Ok(result.Value);
    }

    private static IResult Problem(
        ControlledSerdeFailure failure)
    {
        var status = failure.Category switch
        {
            ControlledSerdeFailureCategory.InvalidRequest =>
                StatusCodes.Status400BadRequest,
            ControlledSerdeFailureCategory.Unsupported =>
                StatusCodes.Status501NotImplemented,
            ControlledSerdeFailureCategory.MalformedInput =>
                StatusCodes.Status422UnprocessableEntity,
            ControlledSerdeFailureCategory.BoundExceeded =>
                StatusCodes.Status422UnprocessableEntity,
            ControlledSerdeFailureCategory.Cancelled =>
                StatusCodes.Status408RequestTimeout,
            ControlledSerdeFailureCategory.Timeout =>
                StatusCodes.Status504GatewayTimeout,
            _ => StatusCodes.Status500InternalServerError,
        };

        return Results.Problem(
            statusCode: status,
            type:
                $"urn:kafdeck:problem:{failure.Code}",
            title:
                "Controlled SerDe operation failed",
            detail:
                failure.SafeMessage,
            extensions:
                new Dictionary<string, object?>
                {
                    ["code"] = failure.Code,
                });
    }

    private static IResult Invalid(
        string detail) =>
        Results.Problem(
            statusCode:
                StatusCodes.Status400BadRequest,
            type:
                "urn:kafdeck:problem:serde-request-invalid",
            title:
                "Controlled SerDe request is invalid",
            detail:
                detail);
}
