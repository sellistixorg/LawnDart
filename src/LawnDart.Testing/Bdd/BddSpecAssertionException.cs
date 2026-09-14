namespace LawnDart.Testing.Bdd;

/// <summary>
/// Thrown when an <see cref="AggregateSpec"/> assertion fails inside the spec runner.
/// </summary>
/// <remarks>
/// Distinct from <see cref="InvalidOperationException"/> so
/// <c>Assert.ThrowsAsync&lt;InvalidOperationException&gt;(() =&gt; spec.RunAsync())</c>
/// cannot pass when the spec itself failed rather than a domain rule.
/// This type does not derive from <see cref="InvalidOperationException"/>.
/// </remarks>
public sealed class BddSpecAssertionException : Exception
{
    public BddSpecAssertionException(string message)
        : base(message)
    {
    }

    public BddSpecAssertionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
