using System.Security.Cryptography;
using System.Text;

namespace Kafdeck.Modules.Administration;

public sealed class HmacMutationMaterialDigestService : IMutationMaterialDigestService, IDisposable
{
    private readonly byte[] _key;
    private bool _disposed;

    public HmacMutationMaterialDigestService(string keyMaterial)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyMaterial);
        _key = Encoding.UTF8.GetBytes(keyMaterial);
        if (_key.Length < 32)
        {
            CryptographicOperations.ZeroMemory(_key);
            throw new ArgumentOutOfRangeException(
                nameof(keyMaterial),
                "Mutation material-digest key must contain at least 32 UTF-8 bytes.");
        }
    }

    public string ComputeDigest(ReadOnlySpan<byte> material)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Convert.ToHexString(HMACSHA256.HashData(_key, material)).ToLowerInvariant();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        CryptographicOperations.ZeroMemory(_key);
        _disposed = true;
    }
}
