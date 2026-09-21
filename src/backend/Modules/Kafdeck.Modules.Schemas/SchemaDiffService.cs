using Kafdeck.Core.Records;

namespace Kafdeck.Modules.Schemas;

public sealed record SchemaDiffHunk(
    int LeftStartLine,
    int LeftLineCount,
    int RightStartLine,
    int RightLineCount,
    IReadOnlyList<string> RemovedLines,
    IReadOnlyList<string> AddedLines);

public sealed record SchemaDiffResult(
    bool IsEqual,
    IReadOnlyList<SchemaDiffHunk> Hunks);

public sealed class SchemaDiffService
{
    public const int MaxSchemaCharacters = 1_048_576;
    public const int MaxSchemaLines = 4_096;

    public SchemaDiffResult Compare(
        RecordSchemaDocument left,
        RecordSchemaDocument right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);

        var leftLines = Normalize(left.SchemaText);
        var rightLines = Normalize(right.SchemaText);

        if (leftLines.SequenceEqual(rightLines, StringComparer.Ordinal))
        {
            return new SchemaDiffResult(true, Array.Empty<SchemaDiffHunk>());
        }

        var prefix = 0;
        var common = Math.Min(leftLines.Length, rightLines.Length);
        while (prefix < common &&
               string.Equals(leftLines[prefix], rightLines[prefix], StringComparison.Ordinal))
        {
            prefix++;
        }

        var leftSuffix = leftLines.Length - 1;
        var rightSuffix = rightLines.Length - 1;
        while (leftSuffix >= prefix &&
               rightSuffix >= prefix &&
               string.Equals(leftLines[leftSuffix], rightLines[rightSuffix], StringComparison.Ordinal))
        {
            leftSuffix--;
            rightSuffix--;
        }

        var removed = leftSuffix < prefix
            ? Array.Empty<string>()
            : leftLines[prefix..(leftSuffix + 1)];
        var added = rightSuffix < prefix
            ? Array.Empty<string>()
            : rightLines[prefix..(rightSuffix + 1)];

        return new SchemaDiffResult(
            false,
            [
                new SchemaDiffHunk(
                    prefix + 1,
                    removed.Length,
                    prefix + 1,
                    added.Length,
                    removed,
                    added),
            ]);
    }

    private static string[] Normalize(string schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        if (schema.Length > MaxSchemaCharacters)
        {
            throw new ArgumentOutOfRangeException(nameof(schema), "Schema exceeds the diff character bound.");
        }

        var normalized = schema
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace('\r', '\n');

        var lines = normalized
            .Split('\n')
            .Select(line => line.TrimEnd())
            .ToArray();

        if (lines.Length > MaxSchemaLines)
        {
            throw new ArgumentOutOfRangeException(nameof(schema), "Schema exceeds the diff line bound.");
        }

        return lines;
    }
}
