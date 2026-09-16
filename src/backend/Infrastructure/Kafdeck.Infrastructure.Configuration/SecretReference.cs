namespace Kafdeck.Infrastructure.Configuration;

public enum SecretReferenceKind
{
    Environment = 1,
    File = 2,
}

public sealed class SecretReference
{
    private SecretReference(SecretReferenceKind kind, string locator)
    {
        Kind = kind;
        Locator = locator;
    }

    public SecretReferenceKind Kind { get; }

    internal string Locator { get; }

    public static SecretReference Parse(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new KafdeckConfigurationException("Secret reference must not be empty.");
        }

        if (value.StartsWith("env:", StringComparison.Ordinal))
        {
            var locator = value[4..];
            if (!IsValidEnvironmentVariableName(locator))
            {
                throw new KafdeckConfigurationException("Environment secret reference is invalid.");
            }

            return new SecretReference(SecretReferenceKind.Environment, locator);
        }

        if (value.StartsWith("file:", StringComparison.Ordinal))
        {
            var locator = value[5..];
            if (string.IsNullOrWhiteSpace(locator) || !Path.IsPathFullyQualified(locator))
            {
                throw new KafdeckConfigurationException("File secret reference must use an absolute mounted path.");
            }

            return new SecretReference(SecretReferenceKind.File, locator);
        }

        throw new KafdeckConfigurationException("Secret references must use the env: or file: scheme.");
    }

    public override string ToString() => "[redacted-secret-reference]";

    private static bool IsValidEnvironmentVariableName(string value)
    {
        if (string.IsNullOrEmpty(value) || !(char.IsLetter(value[0]) || value[0] == '_'))
        {
            return false;
        }

        for (var index = 1; index < value.Length; index++)
        {
            var character = value[index];
            if (!(char.IsLetterOrDigit(character) || character == '_'))
            {
                return false;
            }
        }

        return true;
    }
}

public sealed class SecretValue
{
    private readonly string _value;

    internal SecretValue(string value) => _value = value;

    public string Reveal() => _value;

    public override string ToString() => "[redacted-secret]";
}
