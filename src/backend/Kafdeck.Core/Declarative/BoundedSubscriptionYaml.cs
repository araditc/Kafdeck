using System.Text.Json;

namespace Kafdeck.Core.Declarative;

// A finite typed YAML subset, not a general YAML object-graph deserializer.
internal static class BoundedSubscriptionYaml
{
    internal static byte[] ToJson(string text)
    {
        var lines = text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        if (lines.Length > DeclarativeSubscriptionManifest.MaxNodes || text.Contains('\r'))
        {
            // CRLF is accepted; a bare CR is not a line boundary in this grammar.
            if (lines.Length > DeclarativeSubscriptionManifest.MaxNodes || text.Replace("\r\n", "", StringComparison.Ordinal).Contains('\r')) throw Invalid();
        }
        // Whole-source graph-token rejection precedes every map/list construction.
        foreach (var line in lines) _ = Clean(line);
        var root = new Dictionary<string, object>(StringComparer.Ordinal);
        Dictionary<string, object>? section = null;
        string? sectionName = null;
        List<string>? sequence = null;
        foreach (var raw in lines)
        {
            var line = Clean(raw); if (string.IsNullOrWhiteSpace(line)) continue;
            var indent = line.Length - line.TrimStart(' ').Length;
            var content = line[indent..];
            if (indent == 4 && content.StartsWith("- ", StringComparison.Ordinal) && sequence is not null)
            {
                if (sequence.Count >= 32) throw Invalid();
                sequence.Add(Scalar(content[2..])); continue;
            }
            sequence = null;
            if (indent is not (0 or 2)) throw Invalid();
            var colon = content.IndexOf(':');
            if (colon < 1) throw Invalid();
            var key = content[..colon]; var value = content[(colon + 1)..].Trim();
            if (key.Any(ch => !char.IsAsciiLetter(ch))) throw Invalid();
            if (indent == 0)
            {
                section = null; sectionName = null;
                if (key is "metadata" or "spec")
                {
                    if (value.Length != 0) throw Invalid();
                    section = new(StringComparer.Ordinal); sectionName = key;
                    if (!root.TryAdd(key, section)) throw Invalid();
                }
                else
                {
                    if (key is not ("apiVersion" or "kind") || !root.TryAdd(key, Scalar(value))) throw Invalid();
                }
                continue;
            }
            if (section is null || sectionName is null) throw Invalid();
            if (sectionName == "metadata" && key is not ("deploymentRef" or "name" or "ownerRef") ||
                sectionName == "spec" && key is not ("destinationRef" or "state" or "eventClasses" or "eventTypes")) throw Invalid();
            if (key is "eventClasses" or "eventTypes")
            {
                sequence = value.Length == 0 ? [] : FlowSequence(value);
                if (!section.TryAdd(key, sequence)) throw Invalid();
            }
            else if (!section.TryAdd(key, Scalar(value))) throw Invalid();
        }
        return JsonSerializer.SerializeToUtf8Bytes(root);
    }

    private static string Clean(string line)
    {
        if (line.Length > 2048 || line.Any(ch => char.IsControl(ch))) throw Invalid();
        var quote = '\0'; var escaped = false;
        for (var i = 0; i < line.Length; i++)
        {
            var ch = line[i];
            if (quote == '"')
            {
                if (escaped) { escaped = false; continue; }
                if (ch == '\\') { escaped = true; continue; }
                if (ch == '"') quote = '\0';
                continue;
            }
            if (quote == '\'')
            {
                if (ch == '\'' && i + 1 < line.Length && line[i + 1] == '\'') { i++; continue; }
                if (ch == '\'') quote = '\0';
                continue;
            }
            if (ch == '#' && (i == 0 || char.IsWhiteSpace(line[i - 1]))) return line[..i].TrimEnd();
            if (ch is '"' or '\'') { quote = ch; continue; }
            if (ch is '&' or '*' or '!' or '{' or '}' or '|' or '>' or '<' or '%' or '`') throw Invalid();
        }
        if (quote != '\0') throw Invalid();
        return line.TrimEnd();
    }

    private static string Scalar(string text)
    {
        text = text.Trim(); if (text.Length is < 1 or > 1024) throw Invalid();
        if (text[0] == '"') return JsonSerializer.Deserialize<string>(text) ?? throw Invalid();
        if (text[0] == '\'')
        {
            if (text.Length < 2 || text[^1] != '\'') throw Invalid();
            return text[1..^1].Replace("''", "'", StringComparison.Ordinal);
        }
        if (text.Any(ch => !(char.IsAsciiLetterOrDigit(ch) || ch is '.' or '_' or '-' or '/'))) throw Invalid();
        return text;
    }
    private static List<string> FlowSequence(string text)
    {
        if (text.Length < 2 || text[0] != '[' || text[^1] != ']') throw Invalid();
        if (text[1..^1].Trim().Length == 0) return [];
        // Admitted values contain no comma; quoted commas cannot become valid filters.
        var parts = text[1..^1].Split(',');
        if (parts.Length > 32) throw Invalid();
        return parts.Select(Scalar).ToList();
    }
    private static ArgumentException Invalid() => DeclarativeSubscriptionManifest.Invalid();
}
