namespace Kafdeck.Infrastructure.Configuration;

public sealed class SecretResolver
{
    public SecretValue Resolve(SecretReference reference)
    {
        ArgumentNullException.ThrowIfNull(reference);

        string? value;
        switch (reference.Kind)
        {
            case SecretReferenceKind.Environment:
                value = Environment.GetEnvironmentVariable(reference.Locator);
                if (string.IsNullOrEmpty(value))
                {
                    throw new KafdeckConfigurationException("Environment-backed secret is not available.");
                }

                break;

            case SecretReferenceKind.File:
                try
                {
                    value = File.ReadAllText(reference.Locator).TrimEnd('\r', '\n');
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
                {
                    throw new KafdeckConfigurationException("Mounted secret file could not be read.", exception);
                }

                if (string.IsNullOrEmpty(value))
                {
                    throw new KafdeckConfigurationException("Mounted secret file is empty.");
                }

                break;

            default:
                throw new KafdeckConfigurationException("Unsupported secret reference kind.");
        }

        return new SecretValue(value);
    }

    /// <summary>
    /// Finite, strict UTF-8 resolution for notification credentials. The
    /// existing Resolve API is intentionally unchanged for other features.
    /// File bytes are capped during the read, including optional UTF-8 BOM
    /// and a single CRLF, rather than trusted from file metadata.
    /// </summary>
    public SecretValue ResolveBounded(
        SecretReference reference,
        int maxUtf8Bytes)
    {
        ArgumentNullException.ThrowIfNull(reference);
        if (maxUtf8Bytes is < 1 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(maxUtf8Bytes));
        }

        string value;
        switch (reference.Kind)
        {
            case SecretReferenceKind.Environment:
                value = Environment.GetEnvironmentVariable(reference.Locator)
                    ?? throw new KafdeckConfigurationException(
                        "Environment-backed secret is not available.");
                if (value.Length == 0)
                {
                    throw new KafdeckConfigurationException(
                        "Environment-backed secret is not available.");
                }
                if (System.Text.Encoding.UTF8.GetByteCount(value) > maxUtf8Bytes)
                {
                    throw new KafdeckConfigurationException(
                        "Notification credential exceeds the bounded size limit.");
                }
                break;

            case SecretReferenceKind.File:
                value = ReadBoundedFileSecret(reference.Locator, maxUtf8Bytes);
                if (value.Length == 0)
                {
                    throw new KafdeckConfigurationException(
                        "Mounted secret file is empty.");
                }
                break;

            default:
                throw new KafdeckConfigurationException(
                    "Unsupported secret reference kind.");
        }

        return new SecretValue(value);
    }

    private static string ReadBoundedFileSecret(string path, int maxUtf8Bytes)
    {
        const int maxFramingBytes = 5; // UTF-8 BOM (3) + CRLF (2).
        var buffer = new byte[maxUtf8Bytes + maxFramingBytes + 1];
        try
        {
            using var stream = new FileStream(path, FileMode.Open,
                FileAccess.Read, FileShare.Read, 4096,
                FileOptions.SequentialScan);
            var count = 0;
            while (count < buffer.Length)
            {
                var read = stream.Read(buffer, count, buffer.Length - count);
                if (read == 0)
                {
                    break;
                }
                count += read;
            }

            if (count > maxUtf8Bytes + maxFramingBytes)
            {
                throw new KafdeckConfigurationException(
                    "Mounted notification credential file exceeds the bounded size limit.");
            }

            var bom = count >= 3 && buffer[0] == 0xEF &&
                buffer[1] == 0xBB && buffer[2] == 0xBF ? 3 : 0;
            string value;
            try
            {
                value = new System.Text.UTF8Encoding(false, true)
                    .GetString(buffer, bom, count - bom)
                    .TrimEnd('\r', '\n');
            }
            catch (System.Text.DecoderFallbackException)
            {
                throw new KafdeckConfigurationException(
                    "Mounted notification credential must contain valid UTF-8.");
            }

            if (System.Text.Encoding.UTF8.GetByteCount(value) > maxUtf8Bytes)
            {
                throw new KafdeckConfigurationException(
                    "Mounted notification credential exceeds the bounded size limit.");
            }

            return value;
        }
        catch (Exception exception)
            when (exception is IOException or UnauthorizedAccessException)
        {
            // Do not attach the OS exception: it can expose the secret path.
            throw new KafdeckConfigurationException(
                "Mounted notification credential file could not be read.");
        }
        finally
        {
            Array.Clear(buffer);
        }
    }
}
