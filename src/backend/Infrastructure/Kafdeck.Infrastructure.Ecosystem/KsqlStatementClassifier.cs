namespace Kafdeck.Infrastructure.Ecosystem;

internal enum KsqlStatementAdmissionOutcome
{
    Allowed = 1,
    Invalid = 2,
    Unsupported = 3,
}

internal sealed record KsqlStatementAdmission(
    KsqlStatementAdmissionOutcome Outcome,
    string Code,
    string SafeMessage,
    string? CanonicalStatement)
{
    public bool IsAllowed =>
        Outcome == KsqlStatementAdmissionOutcome.Allowed &&
        CanonicalStatement is not null;
}

internal static class KsqlStatementClassifier
{
    public const int MaxStatementCharacters = 64 * 1024;

    private static readonly HashSet<string> ForbiddenTokens =
        new(StringComparer.Ordinal)
        {
            "CREATE",
            "DROP",
            "ALTER",
            "INSERT",
            "TERMINATE",
            "RUN",
            "SCRIPT",
            "DELETE",
            "UPDATE",
            "MERGE",
            "SET",
            "UNSET",
            "DEFINE",
            "UNDEFINE",
        };

    public static KsqlStatementAdmission Classify(string? statement)
    {
        if (string.IsNullOrWhiteSpace(statement))
        {
            return Invalid(
                "ksql_statement_required",
                "A non-empty ksqlDB statement is required.");
        }

        var canonical = statement.Trim();
        if (canonical.Length > MaxStatementCharacters ||
            canonical.Any(character =>
                char.IsControl(character) &&
                character is not '\r' and not '\n' and not '\t'))
        {
            return Invalid(
                "ksql_statement_invalid",
                "ksqlDB statement exceeds the admitted text boundary.");
        }

        if (canonical.Contains("${", StringComparison.Ordinal))
        {
            return Unsupported(
                "ksql_session_variables_not_admitted",
                "ksqlDB session-variable substitution is not admitted.");
        }

        var scan = Scan(canonical);
        if (!scan.IsValid)
        {
            return Invalid(scan.Code, scan.Message);
        }

        if (scan.Tokens.Count == 0 ||
            !string.Equals(scan.Tokens[0], "SELECT", StringComparison.Ordinal))
        {
            return Unsupported(
                "ksql_statement_not_read_query",
                "Only bounded SELECT statements are admitted.");
        }

        var forbidden = scan.Tokens.FirstOrDefault(ForbiddenTokens.Contains);
        if (forbidden is not null)
        {
            return Unsupported(
                "ksql_statement_not_admitted",
                $"ksqlDB token '{forbidden}' is not admitted by the read-query policy.");
        }

        return new KsqlStatementAdmission(
            KsqlStatementAdmissionOutcome.Allowed,
            "ksql_select_admitted",
            "Bounded SELECT statement admitted.",
            canonical);
    }

    private static ScanResult Scan(string statement)
    {
        var tokens = new List<string>();
        var token = new System.Text.StringBuilder();
        var semicolonSeen = false;
        var state = LexState.Normal;

        void FlushToken()
        {
            if (token.Length == 0)
            {
                return;
            }

            tokens.Add(token.ToString().ToUpperInvariant());
            token.Clear();
        }

        for (var index = 0; index < statement.Length; index++)
        {
            var current = statement[index];
            var next = index + 1 < statement.Length
                ? statement[index + 1]
                : '\0';

            switch (state)
            {
                case LexState.LineComment:
                    if (current is '\r' or '\n')
                    {
                        state = LexState.Normal;
                    }
                    continue;

                case LexState.BlockComment:
                    if (current == '*' && next == '/')
                    {
                        state = LexState.Normal;
                        index++;
                    }
                    continue;

                case LexState.SingleQuoted:
                    if (current == '\'' && next == '\'')
                    {
                        index++;
                        continue;
                    }
                    if (current == '\'')
                    {
                        state = LexState.Normal;
                    }
                    continue;

                case LexState.DoubleQuoted:
                    if (current == '"' && next == '"')
                    {
                        index++;
                        continue;
                    }
                    if (current == '"')
                    {
                        state = LexState.Normal;
                    }
                    continue;

                case LexState.BacktickQuoted:
                    if (current == '`' && next == '`')
                    {
                        index++;
                        continue;
                    }
                    if (current == '`')
                    {
                        state = LexState.Normal;
                    }
                    continue;
            }

            if (current == '-' && next == '-')
            {
                FlushToken();
                state = LexState.LineComment;
                index++;
                continue;
            }

            if (current == '/' && next == '*')
            {
                FlushToken();
                state = LexState.BlockComment;
                index++;
                continue;
            }

            if (current == '\'')
            {
                FlushToken();
                state = LexState.SingleQuoted;
                continue;
            }

            if (current == '"')
            {
                FlushToken();
                state = LexState.DoubleQuoted;
                continue;
            }

            if (current == '`')
            {
                FlushToken();
                state = LexState.BacktickQuoted;
                continue;
            }

            if (current == ';')
            {
                FlushToken();
                if (semicolonSeen ||
                    HasExecutableTextAfter(statement, index + 1))
                {
                    return ScanResult.Invalid(
                        "ksql_multiple_statements_not_admitted",
                        "Only one ksqlDB statement is admitted per request.");
                }

                semicolonSeen = true;
                continue;
            }

            if (semicolonSeen && !char.IsWhiteSpace(current))
            {
                return ScanResult.Invalid(
                    "ksql_multiple_statements_not_admitted",
                    "Only one ksqlDB statement is admitted per request.");
            }

            if (char.IsLetterOrDigit(current) || current == '_')
            {
                token.Append(current);
            }
            else
            {
                FlushToken();
            }
        }

        FlushToken();

        if (state is LexState.BlockComment or
            LexState.SingleQuoted or
            LexState.DoubleQuoted or
            LexState.BacktickQuoted)
        {
            return ScanResult.Invalid(
                "ksql_statement_unterminated_literal",
                "ksqlDB statement contains an unterminated comment or quoted literal.");
        }

        return ScanResult.Valid(tokens);
    }

    private static bool HasExecutableTextAfter(string statement, int start)
    {
        var state = LexState.Normal;
        for (var index = start; index < statement.Length; index++)
        {
            var current = statement[index];
            var next = index + 1 < statement.Length
                ? statement[index + 1]
                : '\0';

            if (state == LexState.LineComment)
            {
                if (current is '\r' or '\n')
                {
                    state = LexState.Normal;
                }
                continue;
            }

            if (state == LexState.BlockComment)
            {
                if (current == '*' && next == '/')
                {
                    state = LexState.Normal;
                    index++;
                }
                continue;
            }

            if (char.IsWhiteSpace(current))
            {
                continue;
            }

            if (current == '-' && next == '-')
            {
                state = LexState.LineComment;
                index++;
                continue;
            }

            if (current == '/' && next == '*')
            {
                state = LexState.BlockComment;
                index++;
                continue;
            }

            return true;
        }

        return state == LexState.BlockComment;
    }

    private static KsqlStatementAdmission Invalid(string code, string message) =>
        new(KsqlStatementAdmissionOutcome.Invalid, code, message, null);

    private static KsqlStatementAdmission Unsupported(string code, string message) =>
        new(KsqlStatementAdmissionOutcome.Unsupported, code, message, null);

    private enum LexState
    {
        Normal = 0,
        LineComment = 1,
        BlockComment = 2,
        SingleQuoted = 3,
        DoubleQuoted = 4,
        BacktickQuoted = 5,
    }

    private sealed record ScanResult(
        bool IsValid,
        string Code,
        string Message,
        IReadOnlyList<string> Tokens)
    {
        public static ScanResult Valid(IReadOnlyList<string> tokens) =>
            new(true, "ok", "ok", tokens);

        public static ScanResult Invalid(string code, string message) =>
            new(false, code, message, Array.Empty<string>());
    }
}
