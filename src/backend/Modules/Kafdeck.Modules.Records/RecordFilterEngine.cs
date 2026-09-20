using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kafdeck.Core.Records;

namespace Kafdeck.Modules.Records;

public sealed class RecordFilterCompilationException : Exception
{
    public RecordFilterCompilationException(string message)
        : base(message)
    {
    }
}

public sealed class RecordFilterPlan
{
    internal RecordFilterPlan(
        RecordPreFilter? preFilter,
        RecordFilterBudget budget,
        FilterExpression? expression)
    {
        PreFilter = preFilter;
        Budget = budget;
        Expression = expression;
    }

    public RecordPreFilter? PreFilter { get; }

    public RecordFilterBudget Budget { get; }

    public bool RequiresStructuredValue => Expression is not null;

    internal FilterExpression? Expression { get; }
}

public static class RecordFilterCompiler
{
    public const int MaxRegexPatternCharacters = 1_024;
    public static readonly TimeSpan RegexTimeout = TimeSpan.FromMilliseconds(50);

    public static RecordFilterPlan Compile(RecordFilterRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (request.StructuredFilter is null)
        {
            return new RecordFilterPlan(request.PreFilter, request.Budget, null);
        }

        var source = request.StructuredFilter.Expression;
        if (source.Length > request.Budget.MaxExpressionCharacters)
        {
            throw new RecordFilterCompilationException(
                "Filter expression exceeds the configured character budget.");
        }

        var parser = new FilterParser(
            source,
            request.StructuredFilter.Language,
            request.Budget);

        var expression = parser.Parse();

        var stats = FilterExpressionInspector.Inspect(expression);
        if (stats.NodeCount > request.Budget.MaxAstNodes)
        {
            throw new RecordFilterCompilationException(
                "Filter expression exceeds the configured AST node budget.");
        }

        if (stats.MaxDepth > request.Budget.MaxAstDepth)
        {
            throw new RecordFilterCompilationException(
                "Filter expression exceeds the configured AST depth budget.");
        }

        if (stats.RegexCount > request.Budget.MaxRegexCount)
        {
            throw new RecordFilterCompilationException(
                "Filter expression exceeds the configured regular-expression budget.");
        }

        return new RecordFilterPlan(request.PreFilter, request.Budget, expression);
    }
}

public sealed class RecordFilterEvaluator
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    public bool MatchesPreFilter(KafkaRawRecord record, RecordPreFilter? filter)
    {
        if (filter is null)
        {
            return true;
        }

        if (filter.MinimumOffset.HasValue && record.Offset < filter.MinimumOffset.Value)
        {
            return false;
        }

        if (filter.MaximumOffset.HasValue && record.Offset > filter.MaximumOffset.Value)
        {
            return false;
        }

        if (filter.MinimumTimestampUtc.HasValue &&
            (!record.TimestampUtc.HasValue ||
             record.TimestampUtc.Value < filter.MinimumTimestampUtc.Value))
        {
            return false;
        }

        if (filter.MaximumTimestampUtc.HasValue &&
            (!record.TimestampUtc.HasValue ||
             record.TimestampUtc.Value > filter.MaximumTimestampUtc.Value))
        {
            return false;
        }

        if (filter.KeyEqualsUtf8 is not null &&
            !TryDecodeUtf8(record.Key, out var keyEquals))
        {
            return false;
        }
        else if (filter.KeyEqualsUtf8 is not null &&
                 !string.Equals(keyEquals, filter.KeyEqualsUtf8, StringComparison.Ordinal))
        {
            return false;
        }

        if (filter.KeyPrefixUtf8 is not null &&
            !TryDecodeUtf8(record.Key, out var keyPrefix))
        {
            return false;
        }
        else if (filter.KeyPrefixUtf8 is not null &&
                 !keyPrefix.StartsWith(filter.KeyPrefixUtf8, StringComparison.Ordinal))
        {
            return false;
        }

        foreach (var predicate in filter.Headers)
        {
            var matched = false;

            foreach (var header in record.Headers)
            {
                if (!string.Equals(header.Name, predicate.Name, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!TryDecodeUtf8(header.Value, out var value))
                {
                    continue;
                }

                if (predicate.EqualsUtf8 is not null &&
                    string.Equals(value, predicate.EqualsUtf8, StringComparison.Ordinal))
                {
                    matched = true;
                    break;
                }

                if (predicate.PrefixUtf8 is not null &&
                    value.StartsWith(predicate.PrefixUtf8, StringComparison.Ordinal))
                {
                    matched = true;
                    break;
                }
            }

            if (!matched)
            {
                return false;
            }
        }

        return true;
    }

    public bool MatchesStructuredValue(
        JsonElement structuredValue,
        RecordFilterPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        if (plan.Expression is null)
        {
            return true;
        }

        var context = new EvaluationContext(structuredValue);
        return FilterRuntime.IsTruthy(FilterRuntime.Evaluate(plan.Expression, context));
    }

    public static long EstimateRawBytes(KafkaRawRecord record)
    {
        long total = record.Key?.Length ?? 0;
        total += record.Value?.Length ?? 0;

        foreach (var header in record.Headers)
        {
            total += Encoding.UTF8.GetByteCount(header.Name);
            total += header.Value.Length;
        }

        return total;
    }

    private static bool TryDecodeUtf8(ReadOnlyMemory<byte>? bytes, out string value)
    {
        if (!bytes.HasValue)
        {
            value = string.Empty;
            return false;
        }

        try
        {
            value = StrictUtf8.GetString(bytes.Value.Span);
            return true;
        }
        catch (DecoderFallbackException)
        {
            value = string.Empty;
            return false;
        }
    }

    private static bool TryDecodeUtf8(ReadOnlyMemory<byte> bytes, out string value)
    {
        try
        {
            value = StrictUtf8.GetString(bytes.Span);
            return true;
        }
        catch (DecoderFallbackException)
        {
            value = string.Empty;
            return false;
        }
    }
}

internal abstract record FilterExpression;

internal sealed record LiteralExpression(EvaluationValue Value) : FilterExpression;

internal sealed record PathExpression(
    bool CurrentRoot,
    IReadOnlyList<PathSegment> Segments) : FilterExpression;

internal sealed record UnaryExpression(
    UnaryOperator Operator,
    FilterExpression Operand) : FilterExpression;

internal sealed record BinaryExpression(
    BinaryOperator Operator,
    FilterExpression Left,
    FilterExpression Right) : FilterExpression;

internal sealed record FunctionExpression(
    string Name,
    IReadOnlyList<FilterExpression> Arguments) : FilterExpression;

internal sealed record RegexExpression(
    FilterExpression Target,
    Regex Pattern) : FilterExpression;

internal sealed record PipeExpression(
    FilterExpression Left,
    FilterExpression Right) : FilterExpression;

internal sealed record PathSegment(string? Property, int? Index)
{
    public static PathSegment ForProperty(string property) => new(property, null);

    public static PathSegment ForIndex(int index) => new(null, index);
}

internal enum UnaryOperator
{
    Not = 1,
}

internal enum BinaryOperator
{
    Equal = 1,
    NotEqual = 2,
    GreaterThan = 3,
    GreaterThanOrEqual = 4,
    LessThan = 5,
    LessThanOrEqual = 6,
    And = 7,
    Or = 8,
}

internal enum EvaluationValueKind
{
    Missing = 0,
    Null = 1,
    Boolean = 2,
    Number = 3,
    String = 4,
    Json = 5,
}

internal readonly record struct EvaluationValue(
    EvaluationValueKind Kind,
    object? Value)
{
    public static EvaluationValue Missing => new(EvaluationValueKind.Missing, null);
    public static EvaluationValue Null => new(EvaluationValueKind.Null, null);
    public static EvaluationValue Boolean(bool value) => new(EvaluationValueKind.Boolean, value);
    public static EvaluationValue Number(double value) => new(EvaluationValueKind.Number, value);
    public static EvaluationValue String(string value) => new(EvaluationValueKind.String, value);
    public static EvaluationValue Json(JsonElement value) => new(EvaluationValueKind.Json, value);
}

internal sealed record EvaluationContext(
    JsonElement Root,
    JsonElement? Current = null)
{
    public JsonElement Active => Current ?? Root;

    public EvaluationContext WithCurrent(JsonElement current) => this with { Current = current };
}

internal static class FilterRuntime
{
    public static EvaluationValue Evaluate(
        FilterExpression expression,
        EvaluationContext context) =>
        expression switch
        {
            LiteralExpression literal => literal.Value,
            PathExpression path => ResolvePath(path, context),
            UnaryExpression unary => EvaluateUnary(unary, context),
            BinaryExpression binary => EvaluateBinary(binary, context),
            FunctionExpression function => EvaluateFunction(function, context),
            RegexExpression regex => EvaluateRegex(regex, context),
            PipeExpression pipe => EvaluatePipe(pipe, context),
            _ => EvaluationValue.Missing,
        };

    public static bool IsTruthy(EvaluationValue value) => value.Kind switch
    {
        EvaluationValueKind.Missing => false,
        EvaluationValueKind.Null => false,
        EvaluationValueKind.Boolean => (bool)value.Value!,
        EvaluationValueKind.Number => Math.Abs((double)value.Value!) > double.Epsilon,
        EvaluationValueKind.String => ((string)value.Value!).Length > 0,
        EvaluationValueKind.Json => IsJsonTruthy((JsonElement)value.Value!),
        _ => false,
    };

    private static EvaluationValue ResolvePath(
        PathExpression path,
        EvaluationContext context)
    {
        var current = path.CurrentRoot ? context.Active : context.Root;

        foreach (var segment in path.Segments)
        {
            if (segment.Property is not null)
            {
                if (current.ValueKind != JsonValueKind.Object ||
                    !current.TryGetProperty(segment.Property, out current))
                {
                    return EvaluationValue.Missing;
                }
            }
            else if (segment.Index.HasValue)
            {
                if (current.ValueKind != JsonValueKind.Array ||
                    segment.Index.Value < 0 ||
                    segment.Index.Value >= current.GetArrayLength())
                {
                    return EvaluationValue.Missing;
                }

                current = current[segment.Index.Value];
            }
        }

        return FromJson(current);
    }

    private static EvaluationValue EvaluateUnary(
        UnaryExpression unary,
        EvaluationContext context)
    {
        var operand = Evaluate(unary.Operand, context);

        return unary.Operator switch
        {
            UnaryOperator.Not => EvaluationValue.Boolean(!IsTruthy(operand)),
            _ => EvaluationValue.Missing,
        };
    }

    private static EvaluationValue EvaluateBinary(
        BinaryExpression binary,
        EvaluationContext context)
    {
        if (binary.Operator == BinaryOperator.And)
        {
            var left = Evaluate(binary.Left, context);
            return IsTruthy(left)
                ? EvaluationValue.Boolean(IsTruthy(Evaluate(binary.Right, context)))
                : EvaluationValue.Boolean(false);
        }

        if (binary.Operator == BinaryOperator.Or)
        {
            var left = Evaluate(binary.Left, context);
            return IsTruthy(left)
                ? EvaluationValue.Boolean(true)
                : EvaluationValue.Boolean(IsTruthy(Evaluate(binary.Right, context)));
        }

        var actualLeft = Evaluate(binary.Left, context);
        var actualRight = Evaluate(binary.Right, context);

        return EvaluationValue.Boolean(
            Compare(binary.Operator, actualLeft, actualRight));
    }

    private static EvaluationValue EvaluateFunction(
        FunctionExpression function,
        EvaluationContext context)
    {
        var name = function.Name;

        if (string.Equals(name, "has", StringComparison.Ordinal))
        {
            RequireArity(function, 1);
            return EvaluationValue.Boolean(
                Evaluate(function.Arguments[0], context).Kind != EvaluationValueKind.Missing);
        }

        if (string.Equals(name, "select", StringComparison.Ordinal))
        {
            RequireArity(function, 1);
            return IsTruthy(Evaluate(function.Arguments[0], context))
                ? EvaluationValue.Json(context.Active)
                : EvaluationValue.Missing;
        }

        if (name is "startsWith" or "endsWith" or "contains")
        {
            RequireArity(function, 2);

            var target = Evaluate(function.Arguments[0], context);
            var needle = Evaluate(function.Arguments[1], context);

            if (!TryString(target, out var targetText) ||
                !TryString(needle, out var needleText))
            {
                return EvaluationValue.Boolean(false);
            }

            return name switch
            {
                "startsWith" => EvaluationValue.Boolean(
                    targetText.StartsWith(needleText, StringComparison.Ordinal)),
                "endsWith" => EvaluationValue.Boolean(
                    targetText.EndsWith(needleText, StringComparison.Ordinal)),
                _ => EvaluationValue.Boolean(
                    targetText.Contains(needleText, StringComparison.Ordinal)),
            };
        }

        return EvaluationValue.Missing;
    }

    private static EvaluationValue EvaluateRegex(
        RegexExpression regex,
        EvaluationContext context)
    {
        var value = Evaluate(regex.Target, context);
        return TryString(value, out var text)
            ? EvaluationValue.Boolean(regex.Pattern.IsMatch(text))
            : EvaluationValue.Boolean(false);
    }

    private static EvaluationValue EvaluatePipe(
        PipeExpression pipe,
        EvaluationContext context)
    {
        var left = Evaluate(pipe.Left, context);
        if (left.Kind == EvaluationValueKind.Missing)
        {
            return left;
        }

        if (!TryGetJson(left, out var json))
        {
            return EvaluationValue.Missing;
        }

        return Evaluate(pipe.Right, context.WithCurrent(json));
    }

    private static bool Compare(
        BinaryOperator op,
        EvaluationValue left,
        EvaluationValue right)
    {
        if (op is BinaryOperator.Equal or BinaryOperator.NotEqual)
        {
            var equal = EqualsValue(left, right);
            return op == BinaryOperator.Equal ? equal : !equal;
        }

        if (left.Kind == EvaluationValueKind.Number &&
            right.Kind == EvaluationValueKind.Number)
        {
            var l = (double)left.Value!;
            var r = (double)right.Value!;

            return op switch
            {
                BinaryOperator.GreaterThan => l > r,
                BinaryOperator.GreaterThanOrEqual => l >= r,
                BinaryOperator.LessThan => l < r,
                BinaryOperator.LessThanOrEqual => l <= r,
                _ => false,
            };
        }

        if (TryString(left, out var ls) && TryString(right, out var rs))
        {
            var comparison = string.Compare(ls, rs, StringComparison.Ordinal);
            return op switch
            {
                BinaryOperator.GreaterThan => comparison > 0,
                BinaryOperator.GreaterThanOrEqual => comparison >= 0,
                BinaryOperator.LessThan => comparison < 0,
                BinaryOperator.LessThanOrEqual => comparison <= 0,
                _ => false,
            };
        }

        return false;
    }

    private static bool EqualsValue(
        EvaluationValue left,
        EvaluationValue right)
    {
        if (left.Kind == EvaluationValueKind.Missing ||
            right.Kind == EvaluationValueKind.Missing)
        {
            return false;
        }

        if (left.Kind == EvaluationValueKind.Null ||
            right.Kind == EvaluationValueKind.Null)
        {
            return left.Kind == right.Kind;
        }

        if (left.Kind == EvaluationValueKind.Number &&
            right.Kind == EvaluationValueKind.Number)
        {
            return Math.Abs((double)left.Value! - (double)right.Value!) <= double.Epsilon;
        }

        if (left.Kind == EvaluationValueKind.Boolean &&
            right.Kind == EvaluationValueKind.Boolean)
        {
            return (bool)left.Value! == (bool)right.Value!;
        }

        if (TryString(left, out var ls) && TryString(right, out var rs))
        {
            return string.Equals(ls, rs, StringComparison.Ordinal);
        }

        if (TryGetJson(left, out var lj) && TryGetJson(right, out var rj))
        {
            return JsonElement.DeepEquals(lj, rj);
        }

        return false;
    }

    private static EvaluationValue FromJson(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => EvaluationValue.Null,
        JsonValueKind.True => EvaluationValue.Boolean(true),
        JsonValueKind.False => EvaluationValue.Boolean(false),
        JsonValueKind.Number when value.TryGetDouble(out var number) => EvaluationValue.Number(number),
        JsonValueKind.String => EvaluationValue.String(value.GetString() ?? string.Empty),
        _ => EvaluationValue.Json(value),
    };

    private static bool TryString(EvaluationValue value, out string text)
    {
        if (value.Kind == EvaluationValueKind.String)
        {
            text = (string)value.Value!;
            return true;
        }

        if (value.Kind == EvaluationValueKind.Json)
        {
            var json = (JsonElement)value.Value!;
            if (json.ValueKind == JsonValueKind.String)
            {
                text = json.GetString() ?? string.Empty;
                return true;
            }
        }

        text = string.Empty;
        return false;
    }

    private static bool TryGetJson(EvaluationValue value, out JsonElement json)
    {
        if (value.Kind == EvaluationValueKind.Json)
        {
            json = (JsonElement)value.Value!;
            return true;
        }

        using var document = value.Kind switch
        {
            EvaluationValueKind.Null => JsonDocument.Parse("null"),
            EvaluationValueKind.Boolean => JsonDocument.Parse(
                (bool)value.Value! ? "true" : "false"),
            EvaluationValueKind.Number => JsonDocument.Parse(
                ((double)value.Value!).ToString("R", CultureInfo.InvariantCulture)),
            EvaluationValueKind.String => JsonDocument.Parse(
                JsonSerializer.Serialize((string)value.Value!)),
            _ => null,
        };

        if (document is null)
        {
            json = default;
            return false;
        }

        json = document.RootElement.Clone();
        return true;
    }

    private static bool IsJsonTruthy(JsonElement json) => json.ValueKind switch
    {
        JsonValueKind.Null => false,
        JsonValueKind.False => false,
        JsonValueKind.True => true,
        _ => true,
    };

    private static void RequireArity(FunctionExpression function, int count)
    {
        if (function.Arguments.Count != count)
        {
            throw new InvalidOperationException("Compiled filter function arity is invalid.");
        }
    }
}

internal sealed class FilterParser
{
    private readonly FilterLexer _lexer;
    private readonly RecordFilterLanguage _language;
    private Token _current;

    public FilterParser(
        string source,
        RecordFilterLanguage language,
        RecordFilterBudget budget)
    {
        _language = language;
        _lexer = new FilterLexer(source, budget);
        _current = _lexer.Next();
    }

    public FilterExpression Parse()
    {
        var expression = _language == RecordFilterLanguage.JqStyle
            ? ParsePipe()
            : ParseOr();

        if (_current.Kind != TokenKind.End)
        {
            throw Error("Unexpected token at end of filter expression.");
        }

        return expression;
    }

    private FilterExpression ParsePipe()
    {
        var left = ParseOr();

        while (Match(TokenKind.Pipe))
        {
            var right = ParseOr();
            left = new PipeExpression(left, right);
        }

        return left;
    }

    private FilterExpression ParseOr()
    {
        var left = ParseAnd();

        while (Match(TokenKind.OrOr))
        {
            left = new BinaryExpression(BinaryOperator.Or, left, ParseAnd());
        }

        return left;
    }

    private FilterExpression ParseAnd()
    {
        var left = ParseEquality();

        while (Match(TokenKind.AndAnd))
        {
            left = new BinaryExpression(BinaryOperator.And, left, ParseEquality());
        }

        return left;
    }

    private FilterExpression ParseEquality()
    {
        var left = ParseComparison();

        while (_current.Kind is TokenKind.EqualEqual or TokenKind.NotEqual)
        {
            var op = _current.Kind;
            Advance();

            left = new BinaryExpression(
                op == TokenKind.EqualEqual
                    ? BinaryOperator.Equal
                    : BinaryOperator.NotEqual,
                left,
                ParseComparison());
        }

        return left;
    }

    private FilterExpression ParseComparison()
    {
        var left = ParseUnary();

        while (_current.Kind is
               TokenKind.GreaterThan or
               TokenKind.GreaterThanOrEqual or
               TokenKind.LessThan or
               TokenKind.LessThanOrEqual)
        {
            var op = _current.Kind;
            Advance();

            left = new BinaryExpression(
                op switch
                {
                    TokenKind.GreaterThan => BinaryOperator.GreaterThan,
                    TokenKind.GreaterThanOrEqual => BinaryOperator.GreaterThanOrEqual,
                    TokenKind.LessThan => BinaryOperator.LessThan,
                    _ => BinaryOperator.LessThanOrEqual,
                },
                left,
                ParseUnary());
        }

        return left;
    }

    private FilterExpression ParseUnary()
    {
        if (Match(TokenKind.Bang))
        {
            return new UnaryExpression(UnaryOperator.Not, ParseUnary());
        }

        return ParsePrimary();
    }

    private FilterExpression ParsePrimary()
    {
        if (Match(TokenKind.LeftParen))
        {
            var expression = _language == RecordFilterLanguage.JqStyle
                ? ParsePipe()
                : ParseOr();

            Require(TokenKind.RightParen, "Expected ')' in filter expression.");
            return expression;
        }

        if (_current.Kind == TokenKind.String)
        {
            var value = _current.Text;
            Advance();
            return new LiteralExpression(EvaluationValue.String(value));
        }

        if (_current.Kind == TokenKind.Number)
        {
            if (!double.TryParse(
                    _current.Text,
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out var number) ||
                double.IsInfinity(number) ||
                double.IsNaN(number))
            {
                throw Error("Invalid numeric literal.");
            }

            Advance();
            return new LiteralExpression(EvaluationValue.Number(number));
        }

        if (_current.Kind == TokenKind.True)
        {
            Advance();
            return new LiteralExpression(EvaluationValue.Boolean(true));
        }

        if (_current.Kind == TokenKind.False)
        {
            Advance();
            return new LiteralExpression(EvaluationValue.Boolean(false));
        }

        if (_current.Kind == TokenKind.Null)
        {
            Advance();
            return new LiteralExpression(EvaluationValue.Null);
        }

        if (_current.Kind == TokenKind.Dot)
        {
            return ParsePath(currentRoot: true, firstIdentifier: null);
        }

        if (_current.Kind == TokenKind.Identifier)
        {
            var identifier = _current.Text;
            Advance();

            if (Match(TokenKind.LeftParen))
            {
                return ParseFunction(identifier);
            }

            var currentRoot = false;
            var first = identifier;

            if (string.Equals(identifier, "value", StringComparison.Ordinal))
            {
                first = null;
            }

            return ParsePath(currentRoot, first);
        }

        throw Error("Expected a literal, path, function, or parenthesized expression.");
    }

    private FilterExpression ParseFunction(string name)
    {
        if (_language == RecordFilterLanguage.Cel &&
            string.Equals(name, "select", StringComparison.Ordinal))
        {
            throw Error("select() is available only in jq-style filters.");
        }

        var arguments = new List<FilterExpression>();

        if (!Match(TokenKind.RightParen))
        {
            do
            {
                arguments.Add(_language == RecordFilterLanguage.JqStyle
                    ? ParsePipe()
                    : ParseOr());
            }
            while (Match(TokenKind.Comma));

            Require(TokenKind.RightParen, "Expected ')' after filter function arguments.");
        }

        if (string.Equals(name, "regex", StringComparison.Ordinal))
        {
            if (arguments.Count != 2 ||
                arguments[1] is not LiteralExpression
                {
                    Value.Kind: EvaluationValueKind.String,
                    Value.Value: string pattern,
                })
            {
                throw Error("regex() requires a target and a literal string pattern.");
            }

            if (pattern.Length > RecordFilterCompiler.MaxRegexPatternCharacters)
            {
                throw Error("Regular-expression pattern exceeds the configured bound.");
            }

            try
            {
                var regex = new Regex(
                    pattern,
                    RegexOptions.CultureInvariant | RegexOptions.NonBacktracking,
                    RecordFilterCompiler.RegexTimeout);

                return new RegexExpression(arguments[0], regex);
            }
            catch (ArgumentException)
            {
                throw Error("Regular-expression pattern is invalid or unsupported.");
            }
            catch (NotSupportedException)
            {
                throw Error("Regular-expression pattern uses unsupported constructs.");
            }
        }

        if (name is not ("has" or "startsWith" or "endsWith" or "contains" or "select"))
        {
            throw Error("Filter function is not in the accepted deterministic subset.");
        }

        var required = name is "has" or "select" ? 1 : 2;
        if (arguments.Count != required)
        {
            throw Error("Filter function has an invalid argument count.");
        }

        return new FunctionExpression(name, arguments);
    }

    private FilterExpression ParsePath(
        bool currentRoot,
        string? firstIdentifier)
    {
        var segments = new List<PathSegment>();

        if (firstIdentifier is not null)
        {
            segments.Add(PathSegment.ForProperty(firstIdentifier));
        }

        if (_current.Kind == TokenKind.Dot)
        {
            Advance();
        }

        while (true)
        {
            if (_current.Kind == TokenKind.Identifier)
            {
                segments.Add(PathSegment.ForProperty(_current.Text));
                Advance();

                if (Match(TokenKind.Dot))
                {
                    continue;
                }
            }

            if (Match(TokenKind.LeftBracket))
            {
                if (_current.Kind != TokenKind.Number ||
                    !int.TryParse(
                        _current.Text,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture,
                        out var index) ||
                    index < 0)
                {
                    throw Error("Array path index must be a non-negative integer.");
                }

                Advance();
                Require(TokenKind.RightBracket, "Expected ']' after array path index.");
                segments.Add(PathSegment.ForIndex(index));

                if (Match(TokenKind.Dot))
                {
                    continue;
                }
            }

            break;
        }

        if (segments.Count == 0)
        {
            return new PathExpression(currentRoot, []);
        }

        return new PathExpression(currentRoot, segments);
    }

    private bool Match(TokenKind kind)
    {
        if (_current.Kind != kind)
        {
            return false;
        }

        Advance();
        return true;
    }

    private void Require(TokenKind kind, string message)
    {
        if (!Match(kind))
        {
            throw Error(message);
        }
    }

    private void Advance() => _current = _lexer.Next();

    private RecordFilterCompilationException Error(string message) =>
        new(message);
}

internal static class FilterExpressionInspector
{
    public static FilterExpressionStats Inspect(FilterExpression expression)
    {
        var nodes = 0;
        var maxDepth = 0;
        var regexes = 0;

        Visit(expression, 1, ref nodes, ref maxDepth, ref regexes);
        return new FilterExpressionStats(nodes, maxDepth, regexes);
    }

    private static void Visit(
        FilterExpression expression,
        int depth,
        ref int nodes,
        ref int maxDepth,
        ref int regexes)
    {
        nodes++;
        maxDepth = Math.Max(maxDepth, depth);

        switch (expression)
        {
            case UnaryExpression unary:
                Visit(unary.Operand, depth + 1, ref nodes, ref maxDepth, ref regexes);
                break;

            case BinaryExpression binary:
                Visit(binary.Left, depth + 1, ref nodes, ref maxDepth, ref regexes);
                Visit(binary.Right, depth + 1, ref nodes, ref maxDepth, ref regexes);
                break;

            case FunctionExpression function:
                foreach (var argument in function.Arguments)
                {
                    Visit(argument, depth + 1, ref nodes, ref maxDepth, ref regexes);
                }

                break;

            case RegexExpression regex:
                regexes++;
                Visit(regex.Target, depth + 1, ref nodes, ref maxDepth, ref regexes);
                break;

            case PipeExpression pipe:
                Visit(pipe.Left, depth + 1, ref nodes, ref maxDepth, ref regexes);
                Visit(pipe.Right, depth + 1, ref nodes, ref maxDepth, ref regexes);
                break;
        }
    }
}

internal readonly record struct FilterExpressionStats(
    int NodeCount,
    int MaxDepth,
    int RegexCount);

internal enum TokenKind
{
    End = 0,
    Identifier = 1,
    String = 2,
    Number = 3,
    True = 4,
    False = 5,
    Null = 6,
    Dot = 7,
    LeftParen = 8,
    RightParen = 9,
    LeftBracket = 10,
    RightBracket = 11,
    Comma = 12,
    Bang = 13,
    EqualEqual = 14,
    NotEqual = 15,
    GreaterThan = 16,
    GreaterThanOrEqual = 17,
    LessThan = 18,
    LessThanOrEqual = 19,
    AndAnd = 20,
    OrOr = 21,
    Pipe = 22,
}

internal readonly record struct Token(TokenKind Kind, string Text);

internal sealed class FilterLexer
{
    private readonly string _source;
    private readonly RecordFilterBudget _budget;
    private int _position;

    public FilterLexer(string source, RecordFilterBudget budget)
    {
        _source = source;
        _budget = budget;
    }

    public Token Next()
    {
        SkipWhitespace();

        if (_position >= _source.Length)
        {
            return new Token(TokenKind.End, string.Empty);
        }

        var current = _source[_position];

        if (char.IsLetter(current) || current == '_')
        {
            return ReadIdentifier();
        }

        if (char.IsDigit(current) || current == '-')
        {
            return ReadNumber();
        }

        if (current is '"' or ''')
        {
            return ReadString();
        }

        _position++;

        return current switch
        {
            '.' => new Token(TokenKind.Dot, "."),
            '(' => new Token(TokenKind.LeftParen, "("),
            ')' => new Token(TokenKind.RightParen, ")"),
            '[' => new Token(TokenKind.LeftBracket, "["),
            ']' => new Token(TokenKind.RightBracket, "]"),
            ',' => new Token(TokenKind.Comma, ","),
            '!' when Match('=') => new Token(TokenKind.NotEqual, "!="),
            '!' => new Token(TokenKind.Bang, "!"),
            '=' when Match('=') => new Token(TokenKind.EqualEqual, "=="),
            '>' when Match('=') => new Token(TokenKind.GreaterThanOrEqual, ">="),
            '>' => new Token(TokenKind.GreaterThan, ">"),
            '<' when Match('=') => new Token(TokenKind.LessThanOrEqual, "<="),
            '<' => new Token(TokenKind.LessThan, "<"),
            '&' when Match('&') => new Token(TokenKind.AndAnd, "&&"),
            '|' when Match('|') => new Token(TokenKind.OrOr, "||"),
            '|' => new Token(TokenKind.Pipe, "|"),
            _ => throw Error("Filter expression contains an unsupported character."),
        };
    }

    private Token ReadIdentifier()
    {
        var start = _position++;

        while (_position < _source.Length &&
               (char.IsLetterOrDigit(_source[_position]) || _source[_position] == '_'))
        {
            _position++;
        }

        var text = _source[start.._position];

        return text switch
        {
            "true" => new Token(TokenKind.True, text),
            "false" => new Token(TokenKind.False, text),
            "null" => new Token(TokenKind.Null, text),
            _ => new Token(TokenKind.Identifier, text),
        };
    }

    private Token ReadNumber()
    {
        var start = _position;

        if (_source[_position] == '-')
        {
            _position++;
        }

        var digits = 0;
        while (_position < _source.Length && char.IsDigit(_source[_position]))
        {
            _position++;
            digits++;
        }

        if (_position < _source.Length && _source[_position] == '.')
        {
            _position++;
            while (_position < _source.Length && char.IsDigit(_source[_position]))
            {
                _position++;
                digits++;
            }
        }

        if (digits == 0)
        {
            throw Error("Numeric literal is invalid.");
        }

        if (_position < _source.Length &&
            (_source[_position] is 'e' or 'E'))
        {
            _position++;

            if (_position < _source.Length &&
                (_source[_position] is '+' or '-'))
            {
                _position++;
            }

            var exponentDigits = 0;
            while (_position < _source.Length && char.IsDigit(_source[_position]))
            {
                _position++;
                exponentDigits++;
            }

            if (exponentDigits == 0)
            {
                throw Error("Numeric exponent is invalid.");
            }
        }

        return new Token(TokenKind.Number, _source[start.._position]);
    }

    private Token ReadString()
    {
        var quote = _source[_position++];
        var builder = new StringBuilder();

        while (_position < _source.Length)
        {
            var current = _source[_position++];

            if (current == quote)
            {
                return new Token(TokenKind.String, builder.ToString());
            }

            if (current != '\\')
            {
                builder.Append(current);
                continue;
            }

            if (_position >= _source.Length)
            {
                throw Error("String escape is incomplete.");
            }

            var escaped = _source[_position++];
            builder.Append(escaped switch
            {
                '"' => '"',
                '\'' => '\'',
                '\\' => '\\',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'b' => '\b',
                'f' => '\f',
                _ => throw Error("String escape is unsupported."),
            });

            if (builder.Length > _budget.MaxExpressionCharacters)
            {
                throw Error("String literal exceeds the configured filter bound.");
            }
        }

        throw Error("String literal is not terminated.");
    }

    private bool Match(char expected)
    {
        if (_position >= _source.Length || _source[_position] != expected)
        {
            return false;
        }

        _position++;
        return true;
    }

    private void SkipWhitespace()
    {
        while (_position < _source.Length && char.IsWhiteSpace(_source[_position]))
        {
            _position++;
        }
    }

    private static RecordFilterCompilationException Error(string message) =>
        new(message);
}
