using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kafdeck.Infrastructure.Configuration;

namespace Kafdeck.Api;

internal sealed class OidcTrustedCaHandler : DelegatingHandler
{
    private readonly X509Certificate2 _ca;
    private OidcTrustedCaHandler(X509Certificate2 ca) : base(new SocketsHttpHandler
    {
        AllowAutoRedirect = false, UseProxy = false, ConnectTimeout = TimeSpan.FromSeconds(5),
        SslOptions = new SslClientAuthenticationOptions
        {
            CertificateChainPolicy = new X509ChainPolicy
            {
                TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck,
                CustomTrustStore = { ca },
            },
        },
    }) { _ca = ca; }
    internal static HttpMessageHandler Create(string path)
    {
        try
        {
            using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096);
            var bytes = new byte[16385];
            var count = 0;
            while (count < bytes.Length)
            {
                var read = input.Read(bytes.AsSpan(count));
                if (read == 0) break;
                count += read;
            }
            if (count is < 1 or > 16384) throw new CryptographicException();
            var certificate = X509Certificate2.CreateFromPem(System.Text.Encoding.UTF8.GetString(bytes, 0, count));
            if (!certificate.Extensions.OfType<X509BasicConstraintsExtension>().Any(extension => extension.CertificateAuthority))
            { certificate.Dispose(); throw new CryptographicException(); }
            return new OidcTrustedCaHandler(certificate);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or CryptographicException or ArgumentException)
        { throw new KafdeckConfigurationException("OIDC trusted CA file was refused; certificate contents and path are withheld."); }
    }
    protected override void Dispose(bool disposing) { base.Dispose(disposing); if (disposing) _ca.Dispose(); }
}
