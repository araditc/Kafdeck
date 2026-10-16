using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V08G2ProviderEvidenceTests
{
    [Theory]
    [InlineData("target.count","1")]
    [InlineData("applied.count","1")]
    [InlineData("failed.count","0")]
    [InlineData("verified.count","1")]
    [InlineData("group.count","1")]
    [InlineData("schema.id","1")]
    [InlineData("compatibility.mode","FULL")]
    [InlineData("connector.state","PAUSED")]
    [InlineData("connector.exists","false")]
    [InlineData("delete.permanent","false")]
    [InlineData("delete.scope","SUBJECT")]
    [InlineData("data-job.conflict-obligations","2")]
    [InlineData("data-generator.records","2")]
    [InlineData("verification.state","partial")]
    [InlineData("verification.state","unknown")]
    public void Existing_typed_family_evidence_survives_durable_kernel_normalization(string key,string value)
        => Assert.Equal(value,MutationProviderEvidence.Normalize(new Dictionary<string,string>{{key,value}})[key]);

    [Theory]
    [InlineData("target.count","-1")]
    [InlineData("schema.id","0")]
    [InlineData("connector.state","provider-secret-marker")]
    [InlineData("compatibility.mode","https://provider.invalid/secret")]
    [InlineData("connector.config","secret-marker")]
    [InlineData("delete.scope","caller-invented")]
    [InlineData("verification.state","claimed-success")]
    public void Unknown_fields_and_untyped_values_remain_refused(string key,string value)
        => Assert.Throws<MutationStateException>(()=>MutationProviderEvidence.Normalize(new Dictionary<string,string>{{key,value}}));
}
