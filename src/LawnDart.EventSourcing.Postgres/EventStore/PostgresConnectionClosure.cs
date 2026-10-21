using System.Data;
using System.Net.Sockets;
using LawnDart.EventStore;
using Npgsql;

namespace LawnDart.EventSourcing.Postgres.EventStore;

/// <summary>
/// A pooled connection Postgres has already closed.
/// Classification uses <c>SqlState</c>, exception types, and
/// <see cref="NpgsqlConnection.FullState"/>. It does not read the message text.
/// </summary>
internal static class PostgresConnectionClosure
{
    /// <summary>
    /// True when this failure is a closed session and the write was aborted.
    /// <c>08007</c> is omitted: that code means a commit outcome could not be told.
    /// </summary>
    internal static bool HasRetryableClosureSqlState(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is AppendOutcomeUnknownException
                or ConcurrencyException
                or EventAlreadyAppendedException
                or OperationCanceledException)
                return false;

            if (current is NpgsqlException npgsql && IsRetryableClosureSqlState(npgsql.SqlState))
                return true;
        }

        return false;
    }

    /// <summary>
    /// True for a socket failure. A command timeout is not this.
    /// </summary>
    internal static bool HasTransportBreak(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is AppendOutcomeUnknownException
                or ConcurrencyException
                or EventAlreadyAppendedException
                or OperationCanceledException)
                return false;

            if (current is SocketException or IOException)
                return true;
        }

        return false;
    }

    /// <summary>True when Npgsql has marked this connection broken.</summary>
    internal static bool IsConnectorBroken(NpgsqlConnection connection)
        => connection.FullState == ConnectionState.Broken;

    /// <summary>
    /// True when the failure is a closed connection, including
    /// <c>08007</c> (commit outcome cannot be told).
    /// </summary>
    internal static bool IsClosedConnection(Exception exception, NpgsqlConnection? connection)
    {
        if (connection is not null && IsConnectorBroken(connection))
            return true;

        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is NpgsqlException npgsql && IsClosureSqlState(npgsql.SqlState))
                return true;

            if (current is SocketException or IOException)
                return true;
        }

        return false;
    }

    private static bool IsRetryableClosureSqlState(string? sqlState)
        => IsClosureSqlState(sqlState) && sqlState != "08007";

    private static bool IsClosureSqlState(string? sqlState)
        => sqlState is "57P01" or "57P02" or "57P03"
            || (sqlState is not null && sqlState.StartsWith("08", StringComparison.Ordinal));
}
