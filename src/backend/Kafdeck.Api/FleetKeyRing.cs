using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.DataProtection;

namespace Kafdeck.Api;

internal static class FleetKeyRing
{
    // This directory travels with the installation backup, outside the SQL database.
    internal static IDataProtectionProvider Open(string directory)
    {
        Directory.CreateDirectory(directory);
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(directory,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        if (OperatingSystem.IsWindows())
            return DataProtectionProvider.Create(new DirectoryInfo(directory), builder =>
            {
                builder.SetApplicationName("Kafdeck.Fleet.v1");
                if (OperatingSystem.IsWindows()) builder.ProtectKeysWithDpapi();
            });

        // A local certificate wraps the Data Protection ring. Unix permissions protect
        // its private key. Restoring only the database intentionally cannot decrypt it.
        var certificatePath = Path.Combine(directory, "fleet-key-protection.pfx");
        using (var initialization = new FileStream(Path.Combine(directory, ".initialize"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
        {
            if (!File.Exists(certificatePath))
            {
                using var key = RSA.Create(3072);
                var request = new CertificateRequest("CN=Kafdeck Fleet local storage", key,
                    HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(20));
                var temporary = certificatePath + ".new";
                using (var output = new FileStream(temporary, new FileStreamOptions
                {
                    Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.None,
                    UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
                }))
                {
                    output.Write(certificate.Export(X509ContentType.Pfx));
                    output.Flush(true);
                }
                File.Move(temporary, certificatePath);
            }
        }
        var wrappingCertificate = X509CertificateLoader.LoadPkcs12FromFile(certificatePath, null,
            X509KeyStorageFlags.EphemeralKeySet);
        return DataProtectionProvider.Create(new DirectoryInfo(directory), builder =>
            builder.SetApplicationName("Kafdeck.Fleet.v1").ProtectKeysWithCertificate(wrappingCertificate)
                .UnprotectKeysWithAnyCertificate(wrappingCertificate));
    }
}
