using System.Text.Json;
using System.Xml.Linq;
using Kafdeck.Api;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ReleaseIdentityTests
{
    [Fact]
    public void Packaged_versions_match_the_governed_release_manifest()
    {
        var root = FindRepositoryRoot();

        using var releaseDocument = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(root, ".github", "release", "release.json")));
        var releaseVersion = releaseDocument.RootElement.GetProperty("version").GetString()
                             ?? throw new InvalidOperationException("Release version is missing.");
        var expectedPackageVersion = ToPackageVersion(releaseVersion);

        var buildProps = XDocument.Load(Path.Combine(root, "Directory.Build.props"));
        var backendVersion = buildProps.Descendants("Version").Single().Value;
        var informationalVersion = buildProps.Descendants("InformationalVersion").Single().Value;

        using var packageDocument = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(root, "src", "frontend", "package.json")));
        var frontendVersion = packageDocument.RootElement.GetProperty("version").GetString();

        using var lockDocument = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(root, "src", "frontend", "package-lock.json")));
        var lockVersion = lockDocument.RootElement.GetProperty("version").GetString();
        var lockRootVersion = lockDocument.RootElement
            .GetProperty("packages")
            .GetProperty("")
            .GetProperty("version")
            .GetString();

        Assert.Equal(expectedPackageVersion, backendVersion);
        Assert.Equal(expectedPackageVersion, informationalVersion);
        Assert.Equal(expectedPackageVersion, frontendVersion);
        Assert.Equal(expectedPackageVersion, lockVersion);
        Assert.Equal(expectedPackageVersion, lockRootVersion);
    }

    [Fact]
    public void System_info_reports_read_only_when_mutation_mode_is_disabled()
    {
        var options = CreateOptions(mutationsEnabled: false);

        Assert.Equal("readOnly", KafdeckApiEndpoints.ResolveKafkaAdministrationMode(options));
    }

    [Fact]
    public void System_info_reports_controlled_mutations_when_mutation_mode_is_enabled()
    {
        var options = CreateOptions(mutationsEnabled: true);

        Assert.Equal("controlledMutations", KafdeckApiEndpoints.ResolveKafkaAdministrationMode(options));
    }

    private static KafdeckOptions CreateOptions(bool mutationsEnabled) =>
        new(
            new DeploymentOptions("http://127.0.0.1:8080", null),
            Array.Empty<ClusterProfile>(),
            Administration: new AdministrationOptions(
                new MutationOptions(
                    mutationsEnabled,
                    Persistence: null,
                    MaterialDigestKey: null,
                    PreviewTtl: TimeSpan.FromMinutes(5),
                    MaxConcurrentPerCluster: 1)));

    [Theory]
    [InlineData("v0.6-rc.1", "0.6.0-rc.1")]
    [InlineData("v0.6.1-rc.2", "0.6.1-rc.2")]
    [InlineData("v0.6", "0.6.0")]
    public void Release_version_normalization_preserves_prerelease_identity(
        string releaseVersion,
        string expectedPackageVersion)
    {
        Assert.Equal(expectedPackageVersion, ToPackageVersion(releaseVersion));
    }

    private static string ToPackageVersion(string releaseVersion)
    {
        var value = releaseVersion.TrimStart('v');
        var suffixStart = value.IndexOf('-', StringComparison.Ordinal);
        var numeric = suffixStart >= 0 ? value[..suffixStart] : value;
        var suffix = suffixStart >= 0 ? value[suffixStart..] : string.Empty;
        var componentCount = numeric.Count(character => character == '.') + 1;
        var normalizedNumeric = componentCount == 2 ? numeric + ".0" : numeric;

        return normalizedNumeric + suffix;
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "Kafdeck.slnx")))
            {
                return current.FullName;
            }

            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Unable to locate Kafdeck repository root from test output directory.");
    }
}
