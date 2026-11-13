using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.DataProtection;

namespace Kafdeck.Api;

internal static class ManagedStateDataProtection
{
    // Unconfigured installations retain their legacy ASP.NET key-ring and
    // application discriminator. Restores can select their verified key backup.
    internal static void ConfigureManagedStateDataProtection(this IServiceCollection services, IConfiguration settings)
    {
        var directory = settings["Kafdeck:DataProtection:KeyDirectory"];
        var application = settings["Kafdeck:DataProtection:ApplicationName"];
        if (directory is null && application is null) return;
        if (string.IsNullOrWhiteSpace(directory) || !Path.IsPathFullyQualified(directory) || !Directory.Exists(directory) ||
            (application is not null && (string.IsNullOrWhiteSpace(application) || application.Length > 4096 || application.Any(char.IsControl))))
            throw new KafdeckConfigurationException("Data Protection requires an existing absolute private key directory and a valid optional application name.");
        var protection = services.AddDataProtection().PersistKeysToFileSystem(new DirectoryInfo(directory));
        if (application is not null) protection.SetApplicationName(application);
        // Explicit filesystem persistence must not remove Windows' existing
        // current-user DPAPI protection from newly generated keys.
        if (OperatingSystem.IsWindows()) protection.ProtectKeysWithDpapi();
    }
}
