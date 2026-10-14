using System.Diagnostics;
using NSubstitute;
using RabbitMQ.Client;

namespace LawnDart.Messaging.RabbitMQ.Tests;

public class RabbitMqRecoveryTests
{
    [Fact]
    public async Task Recovery_wait_returns_when_the_connection_is_already_open()
    {
        var connection = Substitute.For<IConnection>();
        connection.IsOpen.Returns(true);

        var started = Stopwatch.StartNew();
        await RabbitMqConnector.WaitForRecoveryAsync(connection, TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.True(started.Elapsed < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Recovery_wait_stops_at_the_timeout()
    {
        var connection = Substitute.For<IConnection>();
        connection.IsOpen.Returns(false);

        var started = Stopwatch.StartNew();
        var timeout = TimeSpan.FromMilliseconds(300);
        var ex = await Assert.ThrowsAsync<TimeoutException>(() =>
            RabbitMqConnector.WaitForRecoveryAsync(connection, timeout, CancellationToken.None));

        Assert.Contains("recovery", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(started.Elapsed >= timeout - TimeSpan.FromMilliseconds(50));
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task Recovery_wait_honours_the_caller_cancellation_token()
    {
        var connection = Substitute.For<IConnection>();
        connection.IsOpen.Returns(false);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            RabbitMqConnector.WaitForRecoveryAsync(connection, TimeSpan.FromSeconds(30), cts.Token));
    }

    [Fact]
    public async Task Recovery_wait_returns_when_the_connection_opens()
    {
        var connection = Substitute.For<IConnection>();
        var open = false;
        connection.IsOpen.Returns(_ => open);

        var waiting = RabbitMqConnector.WaitForRecoveryAsync(
            connection,
            TimeSpan.FromSeconds(5),
            CancellationToken.None);
        await Task.Delay(50);
        open = true;
        await waiting.WaitAsync(TimeSpan.FromSeconds(2));
    }
}
