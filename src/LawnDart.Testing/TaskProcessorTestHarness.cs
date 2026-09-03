using LawnDart.Patterns.TaskProcessing;

namespace LawnDart.Testing;

/// <summary>
/// Test harness for driving an <see cref="ITaskProcessor"/> in isolation.
/// Executes a single poll cycle synchronously (no hosting loop, no delay),
/// making it straightforward to assert what commands would be emitted on a given poll.
/// </summary>
/// <typeparam name="TProcessor">The task processor implementation under test.</typeparam>
public sealed class TaskProcessorTestHarness<TProcessor>
    where TProcessor : class, ITaskProcessor
{
    private readonly TProcessor _processor;

    /// <param name="processor">The task processor instance under test.</param>
    public TaskProcessorTestHarness(TProcessor processor)
    {
        _processor = processor;
    }

    /// <summary>
    /// Executes one poll cycle and returns the emitted commands.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Commands emitted by the processor on this poll.</returns>
    public async Task<IReadOnlyList<ICommand>> PollAsync(CancellationToken cancellationToken = default)
    {
        var commands = await _processor.ProcessTasksAsync(cancellationToken);
        return commands.ToList();
    }
}
