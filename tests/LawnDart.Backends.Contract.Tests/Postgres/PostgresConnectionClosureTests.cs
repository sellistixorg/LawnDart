using System.Net.Sockets;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventStore;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

public sealed class PostgresConnectionClosureTests
{
    [Theory]
    [InlineData("57P01")]
    [InlineData("57P02")]
    [InlineData("57P03")]
    [InlineData("08000")]
    [InlineData("08001")]
    [InlineData("08003")]
    [InlineData("08004")]
    [InlineData("08006")]
    [InlineData("08P01")]
    public void ClosureSqlState_BeforeCommit_IsOneRetry(string sqlState)
    {
        var exception = new PostgresException("closed", "FATAL", "FATAL", sqlState);
        Assert.True(PostgresConnectionClosure.HasRetryableClosureSqlState(exception));
    }

    [Fact]
    public void DatabaseStarting_IsRetried_AndIsRecognizedOnItsOwn()
    {
        var exception = new PostgresException("the database system is starting up", "FATAL", "FATAL", "57P03");
        Assert.True(PostgresConnectionClosure.HasRetryableClosureSqlState(exception));
        Assert.True(PostgresConnectionClosure.IsDatabaseStarting(exception));
        var shutdown = new PostgresException("closed", "FATAL", "FATAL", "57P01");
        Assert.False(PostgresConnectionClosure.IsDatabaseStarting(shutdown));
    }

    [Fact]
    public void TransactionResolutionUnknown_IsNotRetried()
    {
        var exception = new PostgresException("could not tell", "ERROR", "ERROR", "08007");
        Assert.False(PostgresConnectionClosure.HasRetryableClosureSqlState(exception));
        Assert.True(PostgresConnectionClosure.IsClosedConnection(exception, connection: null));
    }

    [Theory]
    [InlineData("23505")]
    [InlineData("28P01")]
    [InlineData("28000")]
    [InlineData("P0001")]
    [InlineData("40001")]
    [InlineData("40P01")]
    [InlineData("42P01")]
    [InlineData("57P05")]
    public void OtherSqlStates_AreNotAClosedConnection(string sqlState)
    {
        var exception = new PostgresException("terminating connection due to administrator command", "ERROR", "ERROR", sqlState);
        Assert.False(PostgresConnectionClosure.HasRetryableClosureSqlState(exception));
        Assert.False(PostgresConnectionClosure.IsClosedConnection(exception, connection: null));
        Assert.False(PostgresConnectionClosure.HasTransportBreak(exception));
    }

    [Fact]
    public void AdminShutdown_UsesSqlState_NotTheMessage()
    {
        var byCode = new PostgresException("password authentication failed", "FATAL", "FATAL", "57P01");
        var byText = new PostgresException("terminating connection due to administrator command", "ERROR", "ERROR", "28P01");
        Assert.True(PostgresConnectionClosure.HasRetryableClosureSqlState(byCode));
        Assert.False(PostgresConnectionClosure.HasRetryableClosureSqlState(byText));
    }

    [Fact]
    public void PlainNpgsqlException_IsNotAClosedConnection()
    {
        var exception = new NpgsqlException("connection reset after the statement was sent");
        Assert.False(PostgresConnectionClosure.HasRetryableClosureSqlState(exception));
        Assert.False(PostgresConnectionClosure.HasTransportBreak(exception));
        Assert.False(PostgresConnectionClosure.IsClosedConnection(exception, connection: null));
    }

    [Fact]
    public void SocketReset_IsATransportBreak_WithoutReadingTheMessage()
    {
        var exception = new NpgsqlException("anything", new SocketException((int)SocketError.ConnectionReset));
        Assert.True(PostgresConnectionClosure.HasTransportBreak(exception));
        Assert.True(PostgresConnectionClosure.IsClosedConnection(exception, connection: null));
        Assert.False(PostgresConnectionClosure.HasRetryableClosureSqlState(exception));
    }

    [Fact]
    public void IoFailure_IsATransportBreak()
    {
        var exception = new NpgsqlException("anything", new IOException("anything"));
        Assert.True(PostgresConnectionClosure.HasTransportBreak(exception));
    }

    [Fact]
    public void Timeout_IsNotATransportBreak()
    {
        var exception = new NpgsqlException("anything", new TimeoutException("anything"));
        Assert.False(PostgresConnectionClosure.HasTransportBreak(exception));
        Assert.False(PostgresConnectionClosure.HasRetryableClosureSqlState(exception));
    }

    [Fact]
    public void VersionConflict_IsNotRetried_EvenWhenTheInnerErrorIsAdminShutdown()
    {
        var inner = new PostgresException("terminating connection due to administrator command", "FATAL", "FATAL", "57P01");
        var conflict = new ConcurrencyException("Expected version 0 but current version is 1", inner, 0, 1);
        Assert.False(PostgresConnectionClosure.HasRetryableClosureSqlState(conflict));
        Assert.False(PostgresConnectionClosure.HasTransportBreak(conflict));
    }
}
