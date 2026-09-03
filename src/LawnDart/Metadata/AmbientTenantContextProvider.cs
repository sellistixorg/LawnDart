namespace LawnDart.Metadata;

/// <summary>
/// An <see cref="ITenantContextProvider"/> backed by <see cref="AsyncLocal{T}"/> that
/// isolates tenant identity per asynchronous execution context.
/// </summary>
/// <remarks>
/// <para>
/// Use this provider for background workers, projection runners, scheduled tasks, and
/// integration adapters that operate without an HTTP request context. Unlike JWT-based
/// resolution, the ambient tenant is set explicitly by the calling code via
/// <see cref="SetTenant"/> and flows transparently through all <c>await</c> continuations
/// within that execution context.
/// </para>
/// <para>
/// Nested scopes are supported: the outer tenant ID is restored when the inner scope
/// is disposed.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// using (ambientProvider.SetTenant("shop"))
/// {
///     await aggregateRepository.HandleCommandAsync(...);
/// }
/// // tenant is null (or restored to outer scope value) here
/// </code>
/// </example>
public sealed class AmbientTenantContextProvider : ITenantContextProvider
{
    private static readonly AsyncLocal<string?> _current = new();

    /// <summary>
    /// Sets the tenant ID for the current asynchronous execution context.
    /// Disposing the returned scope restores the previous tenant ID (or null if there was none).
    /// </summary>
    /// <param name="tenantId">The tenant ID to set.</param>
    /// <returns>A scope that restores the previous tenant ID on disposal.</returns>
    public IDisposable SetTenant(string tenantId)
    {
        var previous = _current.Value;
        _current.Value = tenantId;
        return new TenantScope(previous);
    }

    /// <inheritdoc />
    public string? GetTenantId() => _current.Value;

    /// <inheritdoc />
    public string GetTenantIdRequired()
    {
        var tenantId = _current.Value;
        if (string.IsNullOrWhiteSpace(tenantId))
        {
            throw new InvalidOperationException(
                "Tenant ID is required but no ambient tenant has been set. " +
                "Call AmbientTenantContextProvider.SetTenant() before executing tenant-scoped operations.");
        }
        return tenantId;
    }

    private sealed class TenantScope : IDisposable
    {
        private readonly string? _previous;
        private bool _disposed;

        internal TenantScope(string? previous) => _previous = previous;

        public void Dispose()
        {
            if (!_disposed)
            {
                _current.Value = _previous;
                _disposed = true;
            }
        }
    }
}
