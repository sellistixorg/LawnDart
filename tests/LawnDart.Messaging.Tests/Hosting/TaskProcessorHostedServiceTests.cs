using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.Hosting;
using LawnDart.Patterns.TaskProcessing;

namespace LawnDart.Messaging.Tests.Hosting;

public class TaskProcessorHostedServiceTests
{
    /// <summary>
    /// Upper bound for waiting on a background poll. Generous on purpose: it is a failure deadline,
    /// not an expected duration, so a loaded CI runner does not turn into a false negative.
    /// </summary>
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(30);

    private static TaskProcessorHostedService<TProcessor> BuildService<TProcessor>(
        TProcessor processor,
        TaskProcessorOptions? options = null,
        ICommandDispatcher? dispatcher = null)
        where TProcessor : class, ITaskProcessor
        => new(processor,
            Options.Create(options ?? new TaskProcessorOptions()),
            NullLogger<TaskProcessorHostedService<TProcessor>>.Instance,
            dispatcher);

    [Fact]
    public async Task Start_ThenStop_DoesNotThrow()
    {
        var service = BuildService(new EmptyTaskProcessor(),
            new TaskProcessorOptions { PollingInterval = TimeSpan.FromHours(1) });

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Poll_EmitsCommandsThroughDispatcher()
    {
        var dispatcher = new CapturingCommandDispatcher();
        var processor = new OverdueOrderTaskProcessor("order-1", "order-2");

        // Use a long polling interval so only the first poll runs during the test.
        var service = BuildService(processor,
            new TaskProcessorOptions { PollingInterval = TimeSpan.FromHours(1) },
            dispatcher);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);

        // Wait for both commands to be dispatched. Dispatch happens after the poll returns, so this
        // also proves the poll ran; StartAsync only launches the background task, it does not await it.
        await dispatcher.WaitForDispatchAsync(2, PollTimeout);

        Assert.Equal(1, processor.CallCount);
        Assert.Equal(2, dispatcher.Dispatched.Count);
        Assert.All(dispatcher.Dispatched, c => Assert.IsType<CancelOverdueOrderCommand>(c));

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Poll_NoCommandsEmitted_DispatcherNotCalled()
    {
        var dispatcher = new CapturingCommandDispatcher();
        var processor = new EmptyTaskProcessor();
        var service = BuildService(processor,
            new TaskProcessorOptions { PollingInterval = TimeSpan.FromHours(1) },
            dispatcher);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);

        // Wait for the poll before asserting the dispatcher was not called, otherwise the assertion
        // could pass simply because nothing had happened yet.
        await processor.FirstPollCompleted.WaitAsync(PollTimeout);

        Assert.Empty(dispatcher.Dispatched);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Poll_NoDispatcher_DoesNotThrow()
    {
        var processor = new OverdueOrderTaskProcessor("order-no-dispatch");
        var service = BuildService(processor,
            new TaskProcessorOptions { PollingInterval = TimeSpan.FromHours(1) },
            dispatcher: null);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await processor.FirstPollCompleted.WaitAsync(PollTimeout);

        Assert.Equal(1, processor.CallCount);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }
}
