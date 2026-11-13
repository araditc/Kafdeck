using System.Text.Json;
using System.Text.Json.Nodes;
using Kafdeck.Api;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1ManagedStateFormatTests
{
    [Fact]
    public void Legacy_v09_representation_reads_without_rewriting_and_preserves_optional_receipts()
    {
        using var f = new ManagedProvisioningFixture();
        var original = File.ReadAllBytes(f.StatePath);
        var state = f.Store.Read();
        Assert.Equal(f.OwnerId, state.OwnerId); Assert.Equal(original, File.ReadAllBytes(f.StatePath));
        f.Store.Write(state with { UserProvisionings = [new(Guid.NewGuid(), f.OwnerId, "new@example.test", "viewer", [], new string('a',64), DateTimeOffset.UtcNow)] });
        Assert.Single(f.Store.Read().UserProvisionings!);
        Assert.DoesNotContain("SchemaVersion", File.ReadAllText(f.StatePath), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("root")]
    [InlineData("user")]
    [InlineData("target")]
    [InlineData("service")]
    [InlineData("receipt")]
    public void Unsupported_newer_fields_are_rejected_at_every_persisted_boundary_without_overwrite(string scope)
    {
        using var f = new ManagedProvisioningFixture();
        var compatible = f.Store.Read() with
        {
            Connections = [new("connection-format", "Format", ["127.0.0.1:9092"], "PLAINTEXT", null,
                new("http://127.0.0.1:18081", "none", null, Guid.NewGuid()))],
            UserProvisionings = [new(Guid.NewGuid(), f.OwnerId, "new@example.test", "viewer", [], new string('a',64), DateTimeOffset.UtcNow)]
        };
        var json = JsonSerializer.SerializeToNode(compatible)!.AsObject();
        var node = scope switch
        {
            "root" => json, "user" => json["Users"]![0]!.AsObject(),
            "target" => json["Connections"]![0]!.AsObject(),
            "service" => json["Connections"]![0]!["SchemaRegistry"]!.AsObject(),
            _ => json["UserProvisionings"]![0]!.AsObject()
        };
        node["FutureFormatVersion"] = 2;
        File.WriteAllText(f.StatePath, json.ToJsonString()); var original = File.ReadAllBytes(f.StatePath);
        Assert.Throws<InvalidDataException>(() => f.Store.Read());
        Assert.Throws<InvalidDataException>(() => f.Store.Write(compatible));
        Assert.Equal(original, File.ReadAllBytes(f.StatePath));
        Assert.Empty(Directory.GetFiles(f.DirectoryPath, "*.tmp"));
    }

    [Fact]
    public void Duplicate_properties_are_not_interpreted_by_last_value_wins()
    {
        using var f = new ManagedProvisioningFixture(); var state = f.Store.Read();
        var text = JsonSerializer.Serialize(state).Replace("\"Enabled\":true", "\"Enabled\":false,\"Enabled\":true", StringComparison.Ordinal);
        File.WriteAllText(f.StatePath, text);
        Assert.Throws<InvalidDataException>(() => f.Store.Read());
        Assert.Equal(text, File.ReadAllText(f.StatePath));
    }

    [Fact]
    public void Truncated_state_does_not_reopen_setup_or_replace_the_recovery_input()
    {
        using var f = new ManagedProvisioningFixture(); var state = f.Store.Read();
        File.WriteAllText(f.StatePath, "{\"Issuer\":"); var original = File.ReadAllBytes(f.StatePath);
        Assert.Throws<InvalidDataException>(() => f.Store.IssueCode());
        Assert.Throws<InvalidDataException>(() => f.Store.Write(state));
        Assert.Equal(original, File.ReadAllBytes(f.StatePath));
    }
}
