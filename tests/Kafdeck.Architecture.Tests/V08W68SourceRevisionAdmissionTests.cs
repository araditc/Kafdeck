using Kafdeck.Infrastructure.Persistence;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68SourceRevisionAdmissionTests
{
    [Fact]
    public void Only_exact_trusted_source_and_version_match()
    {
        var sha = new string('a',64);
        var row = new W68SourceRevisionEvidence("owner","source","gen-a",sha,"own-gen");
        bool Check(W68SourceRevisionEvidence? value,string owner="owner",string source="source",
          string generation="gen-a",string rev="",string ownership="own-gen") =>
          W68SourceRevisionAdmission.Matches(value,owner,source,generation,
              rev.Length==0?sha:rev,ownership);
        Assert.True(Check(row));
        Assert.False(Check(null));
        Assert.False(Check(row,owner:"impostor"));
        Assert.False(Check(row,source:"other"));
        Assert.False(Check(row,generation:"gen-b"));
        Assert.False(Check(row,rev:new string('A',64)));
        Assert.False(Check(row,rev:"short"));
        Assert.False(Check(row,ownership:"other"));
    }
}
