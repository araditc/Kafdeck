using System.Security.Cryptography;

namespace Kafdeck.Modules.Administration;

public sealed class MutationExecutionMaterial : IDisposable
{
    private readonly Dictionary<string, byte[]> _items;
    private bool _disposed;

    public MutationExecutionMaterial(
        IReadOnlyDictionary<string, ReadOnlyMemory<byte>>? items = null)
    {
        var source = items ?? new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal);
        if (source.Count > MutationLimits.MaxMaterialDigests)
        {
            throw new ArgumentOutOfRangeException(
                nameof(items),
                $"Execution material must not contain more than {MutationLimits.MaxMaterialDigests} named items.");
        }

        _items = new Dictionary<string, byte[]>(source.Count, StringComparer.Ordinal);
        long totalBytes = 0;

        try
        {
            foreach (var pair in source.OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                var name = RequireName(pair.Key);
                if (pair.Value.Length > MutationLimits.MaxExecutionMaterialItemBytes)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(items),
                        $"Execution material item '{name}' exceeds the per-item byte limit.");
                }

                totalBytes = checked(totalBytes + pair.Value.Length);
                if (totalBytes > MutationLimits.MaxExecutionMaterialTotalBytes)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(items),
                        "Execution material exceeds the total byte limit.");
                }

                var copy = pair.Value.ToArray();
                if (!_items.TryAdd(name, copy))
                {
                    CryptographicOperations.ZeroMemory(copy);
                    throw new ArgumentException(
                        $"Execution material item '{name}' is duplicated.",
                        nameof(items));
                }
            }
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    public int Count
    {
        get
        {
            ThrowIfDisposed();
            return _items.Count;
        }
    }

    public IReadOnlyList<string> Names
    {
        get
        {
            ThrowIfDisposed();
            return _items.Keys.OrderBy(name => name, StringComparer.Ordinal).ToArray();
        }
    }

    public bool TryGet(string name, out ReadOnlyMemory<byte> value)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (_items.TryGetValue(name.Trim(), out var bytes))
        {
            value = bytes;
            return true;
        }

        value = default;
        return false;
    }

    public ReadOnlyMemory<byte> GetRequired(string name)
    {
        if (!TryGet(name, out var value))
        {
            throw new KeyNotFoundException($"Required execution material '{name}' is unavailable.");
        }

        return value;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        foreach (var bytes in _items.Values)
        {
            CryptographicOperations.ZeroMemory(bytes);
        }

        _items.Clear();
        _disposed = true;
    }

    private static string RequireName(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var normalized = value.Trim();
        if (normalized.Length > 256 || normalized.Any(char.IsControl))
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                "Execution material names must be at most 256 characters and contain no control characters.");
        }

        return normalized;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}

public sealed record MutationExecutionContext(
    MutationOperationSnapshot Operation,
    MutationExecutionMaterial Material);
