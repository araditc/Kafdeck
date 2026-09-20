using System.Reflection;
using Kafdeck.Core.Kafka;
using Kafdeck.Core.Records;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class RecordReadContractTests
{
    [Fact]
    public void Default_budget_is_bounded_by_hard_caps()
    {
        var budget = RecordOperationBudget.Default;

        Assert.InRange(budget.MaxRecords, 1, RecordOperationBudget.HardMaxRecords);
        Assert.InRange(budget.MaxRawBytes, 1, RecordOperationBudget.HardMaxRawBytes);
        Assert.InRange(budget.MaxProjectedBytes, 1, RecordOperationBudget.HardMaxProjectedBytes);
        Assert.InRange(budget.MaxDuration, TimeSpan.FromMilliseconds(1), RecordOperationBudget.HardMaxDuration);
        Assert.InRange(budget.MaxRecordsPerSecond, 1, RecordOperationBudget.HardMaxRecordsPerSecond);
    }

    [Fact]
    public void Client_cannot_construct_unbounded_budget()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RecordOperationBudget(
            RecordOperationBudget.HardMaxRecords + 1,
            RecordOperationBudget.DefaultMaxRawBytes,
            RecordOperationBudget.DefaultMaxProjectedBytes,
            RecordOperationBudget.DefaultMaxDuration,
            RecordOperationBudget.DefaultMaxRecordsPerSecond));

        Assert.Throws<ArgumentOutOfRangeException>(() => new RecordOperationBudget(
            RecordOperationBudget.DefaultMaxRecords,
            RecordOperationBudget.HardMaxRawBytes + 1,
            RecordOperationBudget.DefaultMaxProjectedBytes,
            RecordOperationBudget.DefaultMaxDuration,
            RecordOperationBudget.DefaultMaxRecordsPerSecond));

        Assert.Throws<ArgumentOutOfRangeException>(() => new RecordOperationBudget(
            RecordOperationBudget.DefaultMaxRecords,
            RecordOperationBudget.DefaultMaxRawBytes,
            RecordOperationBudget.DefaultMaxProjectedBytes,
            RecordOperationBudget.HardMaxDuration + TimeSpan.FromMilliseconds(1),
            RecordOperationBudget.DefaultMaxRecordsPerSecond));
    }

    [Fact]
    public void Explicit_offset_anchor_rejects_negative_offsets()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RecordAnchor.AtOffset(-1));
        Assert.Equal(42, RecordAnchor.AtOffset(42).Offset);
    }

    [Fact]
    public void Record_request_requires_non_negative_partition()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RecordReadRequest(
            "prod",
            "payments",
            -1,
            RecordAnchor.Earliest(),
            RecordReadDirection.Forward,
            RecordOperationBudget.Default));
    }

    [Fact]
    public void Record_request_rejects_undefined_anchor_kind()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RecordReadRequest(
            "prod",
            "payments",
            0,
            default,
            RecordReadDirection.Forward,
            RecordOperationBudget.Default));
    }

    [Fact]
    public void Record_request_rejects_undefined_direction()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RecordReadRequest(
            "prod",
            "payments",
            0,
            RecordAnchor.Earliest(),
            (RecordReadDirection)0,
            RecordOperationBudget.Default));

        Assert.Throws<ArgumentOutOfRangeException>(() => new RecordReadRequest(
            "prod",
            "payments",
            0,
            RecordAnchor.Earliest(),
            (RecordReadDirection)999,
            RecordOperationBudget.Default));
    }

    [Fact]
    public void Record_read_port_is_read_only_and_cancellable()
    {
        var methods = typeof(IKafkaRecordReadPort).GetMethods(BindingFlags.Instance | BindingFlags.Public);

        Assert.NotEmpty(methods);
        Assert.All(methods, method =>
        {
            Assert.StartsWith("Read", method.Name, StringComparison.Ordinal);

            var parameters = method.GetParameters().Select(parameter => parameter.ParameterType).ToArray();
            Assert.Contains(typeof(KafkaOperationContext), parameters);
            Assert.Contains(typeof(CancellationToken), parameters);

            var forbidden = new[] { "Produce", "Commit", "Subscribe", "Alter", "Delete", "Create", "Reset" };
            Assert.DoesNotContain(forbidden, token =>
                method.Name.Contains(token, StringComparison.OrdinalIgnoreCase));
        });
    }

    [Fact]
    public void Record_contracts_do_not_reference_confluent_kafka()
    {
        var assembly = typeof(IKafkaRecordReadPort).Assembly;

        Assert.DoesNotContain(
            assembly.GetReferencedAssemblies(),
            reference => string.Equals(reference.Name, "Confluent.Kafka", StringComparison.Ordinal));
    }
}
