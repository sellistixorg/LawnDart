namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// Carries the current user and correlation context through async call chains
/// using AsyncLocal so commands, reactors, and event appends all share the same root context.
/// </summary>
public sealed record CorrelationContext(
    string CorrelationId,
    string UserId,
    string UserName,
    string? ReactorName = null)
{
    public bool IsReactorIssued => ReactorName is not null;
}

public static class CorrelationScope
{
    private static readonly AsyncLocal<CorrelationContext?> _current = new();

    public static CorrelationContext? Current => _current.Value;

    public static IDisposable Begin(CorrelationContext ctx)
    {
        var previous = _current.Value;
        _current.Value = ctx;
        return new Disposer(() => _current.Value = previous);
    }

    private sealed class Disposer(Action dispose) : IDisposable
    {
        public void Dispose() => dispose();
    }
}
