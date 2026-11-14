using System.Text.Json;
using System.Xml.Linq;
using Kafdeck.Api;
using Kafdeck.Infrastructure.Configuration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class ReleaseIdentityTests
{
    [Fact]
    public void Packaged_versions_match_the_active_rc_without_rewriting_publication_history()
    {
        var root = FindRepositoryRoot();

        // release.json is the immutable v0.9 publication input, not the current
        // development build selector. Its independent historical checks below remain.
        using var selectionDocument = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(root, ".github", "release", "v1.0-rc.json")));
        var selection = selectionDocument.RootElement;
        var expectedPackageVersion = selection.GetProperty("version").GetString()
                                     ?? throw new InvalidOperationException("RC version is missing.");
        Assert.Equal("v" + expectedPackageVersion, selection.GetProperty("candidate").GetString());
        Assert.False(selection.GetProperty("signed").GetBoolean());
        Assert.False(selection.GetProperty("releaseReady").GetBoolean());
        Assert.False(selection.GetProperty("publicationAuthorized").GetBoolean());
        Assert.Equal(Kafdeck.Mcp.McpServer.ServerVersion, expectedPackageVersion);

        var buildProps = XDocument.Load(Path.Combine(root, "Directory.Build.props"));
        var backendVersion = buildProps.Descendants("Version").Single().Value;
        var informationalVersion = buildProps.Descendants("InformationalVersion").Single().Value;
        var assemblyVersion = buildProps.Descendants("AssemblyVersion").Single().Value;
        var fileVersion = buildProps.Descendants("FileVersion").Single().Value;

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
        Assert.Equal(expectedPackageVersion.Split('-')[0] + ".0", assemblyVersion);
        Assert.Equal(expectedPackageVersion.Split('-')[0] + ".0", fileVersion);
        Assert.Equal(expectedPackageVersion, frontendVersion);
        Assert.Equal(expectedPackageVersion, lockVersion);
        Assert.Equal(expectedPackageVersion, lockRootVersion);
    }

    [Fact]
    public void Readme_release_badge_tracks_actual_GitHub_publication_not_the_prepared_candidate()
    {
        var root = FindRepositoryRoot();

        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        var expectedBadge =
            "[![Latest Release](https://img.shields.io/github/v/release/araditc/Kafdeck)]" +
            "(https://github.com/araditc/Kafdeck/releases/latest)";

        Assert.Contains(expectedBadge, readme, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "img.shields.io/badge/release-",
            readme,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Historical_build_inputs_are_consistent_and_separate_from_publication_status()
    {
        var root = FindRepositoryRoot();
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(root, ".github", "release", "release.json")));
        var manifest = document.RootElement;
        var selected = manifest.GetProperty("version").GetString()!;
        var baseline = manifest.GetProperty("publishedBaselineVersion").GetString()!;
        var status = manifest.GetProperty("status").GetString();
        Assert.Equal("candidate", status); // Publication input, not a registry-status claim.
        Assert.NotEqual(selected, baseline);
        Assert.True(Version.Parse(ToPackageVersion(selected).Split('-')[0]) > Version.Parse(ToPackageVersion(baseline).Split('-')[0]));
        // Candidate metadata is an immutable build input, not current landing-page prose.
        using var publication = JsonDocument.Parse(File.ReadAllText(Path.Combine(root,
            "docs", "implementation", "evidence", "v0.9-release-published-20261112.json")));
        Assert.Equal(selected, publication.RootElement.GetProperty("version").GetString());
        Assert.True(publication.RootElement.GetProperty("releaseReady").GetBoolean());
        Assert.Empty(publication.RootElement.GetProperty("missing").EnumerateArray());
        var readme = File.ReadAllText(Path.Combine(root, "README.md"));
        foreach (var target in new[] { "ROADMAP.md", "docs/operator/v0.9-installation-guide.md",
                     "docs/operator/managed-identity-setup.md", "docs/operator/v0.9-dashboard-guide.md" })
        {
            Assert.Contains($"]({target})", readme, StringComparison.Ordinal);
            Assert.True(File.Exists(Path.Combine(root, target)));
        }
        var notes = manifest.GetProperty("notes").GetString()!;
        Assert.Equal($"docs/releases/{selected}.md", notes);
        Assert.True(File.Exists(Path.Combine(root, notes)));
        Assert.StartsWith($"Kafdeck {selected}", manifest.GetProperty("title").GetString(), StringComparison.Ordinal);
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
    [InlineData("v0.7", "0.7.0")]
    [InlineData("v0.8", "0.8.0")]
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
