using Npgsql;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// Detects a connection string whose host looks like a transaction pooler.
/// A transaction pooler cannot keep a listen session.
/// </summary>
internal static class PostgresPoolerHost
{
    /// <summary>
    /// True when the host contains <c>pooler</c> or <c>pgbouncer</c>, or the port is 6543.
    /// </summary>
    internal static bool LooksLikeTransactionPooler(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        if (builder.Port == 6543)
            return true;

        var host = builder.Host;
        if (string.IsNullOrEmpty(host))
            return false;

        foreach (var part in host.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (part.Contains("pooler", StringComparison.OrdinalIgnoreCase)
                || part.Contains("pgbouncer", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
