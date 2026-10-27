using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68SourceRevisionAdmissionTests
{
    [Fact]
    public void Only_exact_trusted_source_and_version_match()
    {
        var sha = new string('a',64);
        var row = new W68SourceRevisionEvidence("owner","source","11111111-1111-4111-8111-111111111111",sha,"22222222-2222-4222-8222-222222222222");
        bool Check(W68SourceRevisionEvidence? value,string owner="owner",string source="source",
          string generation="11111111-1111-4111-8111-111111111111",string rev="",string ownership="22222222-2222-4222-8222-222222222222") =>
          W68SourceRevisionAdmission.Matches(value,owner,source,generation,
              rev.Length==0?sha:rev,ownership);
        Assert.True(Check(row));
        Assert.False(Check(null));
        Assert.False(Check(row,owner:"impostor"));
        Assert.False(Check(row,source:"other"));
        Assert.False(Check(row,generation:"gen-b"));
        Assert.False(Check(row with { SourceGenerationId = "gen-a" },generation:"gen-a"));
        Assert.False(Check(row with { OwnershipGenerationId = "own-gen" },ownership:"own-gen"));
        Assert.False(Check(row,generation:"11111111-1111-4111-8111-11111111111A"));
        Assert.False(Check(row,generation:"00000000-0000-0000-0000-000000000000"));
        Assert.False(Check(row,rev:new string('A',64)));
        Assert.False(Check(row,rev:"short"));
        Assert.False(Check(row,ownership:"other"));
    }
}
