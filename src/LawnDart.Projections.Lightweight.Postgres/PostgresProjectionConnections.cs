using System.Collections.Concurrent;
using Npgsql;

namespace LawnDart.Projections.Lightweight.Postgres;

internal static class PostgresProjectionConnections
{
    private static readonly ConcurrentDictionary<string, NpgsqlDataSource> Sources = new(StringComparer.Ordinal);

    internal static NpgsqlDataSource Shared(string connectionString)
        => Sources.GetOrAdd(connectionString, static cs => new NpgsqlDataSourceBuilder(cs).Build());

    /// <summary>
    /// Drops idle connections for <paramref name="connectionString"/>.
    /// Call this after a database restart so the next call opens a new connection.
    /// </summary>
    internal static void ClearIdle(string connectionString)
    {
        if (Sources.TryGetValue(connectionString, out var source))
            source.Clear();
    }
}
