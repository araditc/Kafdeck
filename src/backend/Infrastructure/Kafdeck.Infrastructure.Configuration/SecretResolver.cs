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
}
