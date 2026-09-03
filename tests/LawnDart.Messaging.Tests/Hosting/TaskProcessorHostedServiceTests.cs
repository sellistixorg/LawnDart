using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart.Messaging;
using LawnDart.Messaging.Hosting;
using LawnDart.Patterns.TaskProcessing;

namespace LawnDart.Messaging.Tests.Hosting;

public class TaskProcessorHostedServiceTests
{
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

        // Give the first poll time to execute.
        await Task.Delay(200);

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
        var service = BuildService(new EmptyTaskProcessor(),
            new TaskProcessorOptions { PollingInterval = TimeSpan.FromHours(1) },
            dispatcher);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(200);

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
        await Task.Delay(200);

        Assert.Equal(1, processor.CallCount);

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }
}
