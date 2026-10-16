namespace Kafdeck.Cli;

internal static class CliTopicConfiguration
{
    private static readonly HashSet<string> Keys = new(StringComparer.Ordinal)
    {
        "cleanup.policy", "retention.ms", "retention.bytes", "segment.ms", "segment.bytes",
        "min.insync.replicas", "max.message.bytes", "delete.retention.ms", "min.cleanable.dirty.ratio",
    };

    public static void Add(IDictionary<string, string?> changes, string input, bool reset)
    {
        string[] pair = reset ? [input] : input.Split('=', 2);
        if ((!reset && pair.Length != 2) || !Keys.Contains(pair[0]) ||
            !reset && (string.IsNullOrWhiteSpace(pair[1]) || pair[1].Length > 256 || pair[1].Trim() != pair[1] || pair[1].Any(char.IsControl)))
            throw new CliUsageException("Topic configuration requires a closed allowed key and a bounded exact key=value; reset accepts only the key.");
        if (changes.ContainsKey(pair[0]) || changes.Count >= Keys.Count)
            throw new CliUsageException("Topic configuration keys must be unique and fit the nine-key budget.");
        changes.Add(pair[0], reset ? null : pair[1]);
    }
}
