using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;

namespace Kafdeck.Infrastructure.Security;

public static class DeploymentAccessTokenValidator
{
    public static bool Matches(string expectedToken, string? providedToken)
    {
        if (string.IsNullOrEmpty(expectedToken) || string.IsNullOrEmpty(providedToken))
        {
            return false;
        }

        var expectedBytes = Encoding.UTF8.GetBytes(expectedToken);
        var providedBytes = Encoding.UTF8.GetBytes(providedToken);

        try
        {
            return CryptographicOperations.FixedTimeEquals(expectedBytes, providedBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(expectedBytes);
            CryptographicOperations.ZeroMemory(providedBytes);
        }
    }
}

public sealed class FailedAccessAttemptLimiter
{
    private sealed class AttemptWindow(DateTimeOffset startedAtUtc)
    {
        public DateTimeOffset StartedAtUtc { get; set; } = startedAtUtc;

        public int Count { get; set; }
    }

    private readonly ConcurrentDictionary<string, AttemptWindow> _attempts = new(StringComparer.Ordinal);
    private readonly int _maxAttempts;
    private readonly TimeSpan _window;
    private readonly int _maxTrackedClients;

    public FailedAccessAttemptLimiter(int maxAttempts = 10, TimeSpan? window = null, int maxTrackedClients = 4096)
    {
        if (maxAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxAttempts));
        }

        if (maxTrackedClients <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxTrackedClients));
        }

        _maxAttempts = maxAttempts;
        _window = window ?? TimeSpan.FromMinutes(1);
        _maxTrackedClients = maxTrackedClients;
    }

    public bool TryRecordFailure(string clientKey, DateTimeOffset nowUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientKey);

        if (!_attempts.TryGetValue(clientKey, out var attemptWindow))
        {
            if (_attempts.Count >= _maxTrackedClients)
            {
                return false;
            }

            attemptWindow = _attempts.GetOrAdd(clientKey, _ => new AttemptWindow(nowUtc));
        }

        lock (attemptWindow)
        {
            if (nowUtc - attemptWindow.StartedAtUtc >= _window)
            {
                attemptWindow.StartedAtUtc = nowUtc;
                attemptWindow.Count = 0;
            }

            attemptWindow.Count++;
            return attemptWindow.Count <= _maxAttempts;
        }
    }

    public void Reset(string clientKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientKey);
        _attempts.TryRemove(clientKey, out _);
    }
}

public sealed class DeploymentAccessTokenMiddleware
{
    public const string HeaderName = "X-Kafdeck-Access-Token";

    private readonly RequestDelegate _next;
    private readonly string _expectedToken;
    private readonly FailedAccessAttemptLimiter _failureLimiter = new();

    public DeploymentAccessTokenMiddleware(RequestDelegate next, string expectedToken)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _expectedToken = string.IsNullOrEmpty(expectedToken)
            ? throw new ArgumentException("Expected deployment token must not be empty.", nameof(expectedToken))
            : expectedToken;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var clientKey = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
        var providedToken = context.Request.Headers[HeaderName].ToString();

        if (DeploymentAccessTokenValidator.Matches(_expectedToken, providedToken))
        {
            _failureLimiter.Reset(clientKey);
            await _next(context);
            return;
        }

        var withinLimit = _failureLimiter.TryRecordFailure(clientKey, DateTimeOffset.UtcNow);
        context.Response.StatusCode = withinLimit
            ? StatusCodes.Status401Unauthorized
            : StatusCodes.Status429TooManyRequests;

        context.Response.ContentType = "application/problem+json";
        await context.Response.WriteAsJsonAsync(
            new
            {
                type = "about:blank",
                title = withinLimit ? "Unauthorized" : "Too many failed access attempts",
                status = context.Response.StatusCode,
            },
            cancellationToken: context.RequestAborted);
    }
}
