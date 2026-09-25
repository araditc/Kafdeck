using Kafdeck.Core.Kafka;
using Kafdeck.Infrastructure.Kafka;
using Kafdeck.Modules.Administration;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V06W42AclProviderResultTests
{
    [Fact]
    public void Create_mixed_proven_success_and_definitive_failure_is_partial()
    {
        var result = ConfluentKafkaAclMutationResultClassifier
            .ClassifyCreateException(
                successfulCount: 1,
                new[] { KafkaFailureCategory.Unauthorized });

        Assert.Equal(
            MutationExecutionResultKind.PartiallyApplied,
            result.ResultKind);
        Assert.Equal("acl_create_partially_applied", result.ResultCode);
        Assert.Equal("1", result.SafeEvidence!["acknowledged.count"]);
    }

    [Fact]
    public void Create_any_ambiguous_failure_remains_execution_unknown()
    {
        var result = ConfluentKafkaAclMutationResultClassifier
            .ClassifyCreateException(
                successfulCount: 1,
                new[] { KafkaFailureCategory.Timeout });

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            result.ResultKind);
        Assert.Equal("acl_create_result_ambiguous", result.ResultCode);
        Assert.Equal("1", result.SafeEvidence!["acknowledged.count"]);
    }

    [Fact]
    public void Create_all_definitive_failures_prove_nonapplication()
    {
        var result = ConfluentKafkaAclMutationResultClassifier
            .ClassifyCreateException(
                successfulCount: 0,
                new[]
                {
                    KafkaFailureCategory.Unauthorized,
                    KafkaFailureCategory.InvalidRequest,
                });

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            result.ResultKind);
        Assert.Equal("acl_create_failed_definitive", result.ResultCode);
        Assert.Null(result.SafeEvidence);
    }

    [Fact]
    public void Delete_successful_filter_without_deleted_binding_does_not_overclaim_partial_application()
    {
        var result = ConfluentKafkaAclMutationResultClassifier
            .ClassifyDeleteException(
                deletedCount: 0,
                new[] { KafkaFailureCategory.Unauthorized });

        Assert.Equal(
            MutationExecutionResultKind.FailedDefinitive,
            result.ResultKind);
        Assert.Equal("acl_remove_failed_definitive", result.ResultCode);
        Assert.Null(result.SafeEvidence);
    }

    [Fact]
    public void Delete_proven_binding_change_plus_definitive_failure_is_partial()
    {
        var result = ConfluentKafkaAclMutationResultClassifier
            .ClassifyDeleteException(
                deletedCount: 1,
                new[] { KafkaFailureCategory.Unauthorized });

        Assert.Equal(
            MutationExecutionResultKind.PartiallyApplied,
            result.ResultKind);
        Assert.Equal("acl_remove_partially_applied", result.ResultCode);
        Assert.Equal("1", result.SafeEvidence!["acknowledged.count"]);
    }

    [Fact]
    public void Delete_proven_change_plus_ambiguous_failure_remains_unknown()
    {
        var result = ConfluentKafkaAclMutationResultClassifier
            .ClassifyDeleteException(
                deletedCount: 1,
                new[] { KafkaFailureCategory.Unavailable });

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            result.ResultKind);
        Assert.Equal("acl_remove_result_ambiguous", result.ResultCode);
        Assert.Equal("1", result.SafeEvidence!["acknowledged.count"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Exception_without_failure_reports_is_unknown(
        bool create)
    {
        var result = create
            ? ConfluentKafkaAclMutationResultClassifier.ClassifyCreateException(
                successfulCount: 1,
                Array.Empty<KafkaFailureCategory>())
            : ConfluentKafkaAclMutationResultClassifier.ClassifyDeleteException(
                deletedCount: 1,
                Array.Empty<KafkaFailureCategory>());

        Assert.Equal(
            MutationExecutionResultKind.ExecutionUnknown,
            result.ResultKind);
    }
}
