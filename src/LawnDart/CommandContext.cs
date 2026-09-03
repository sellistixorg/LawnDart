using LawnDart.Metadata;

namespace LawnDart;

/// <summary>
/// Context wrapper for commands that includes metadata without polluting the domain model.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public interface ICommandContext<out TCommand> where TCommand : ICommand
{
    /// <summary>
    /// The command.
    /// </summary>
    TCommand Command { get; }

    /// <summary>
    /// Metadata associated with the command.
    /// </summary>
    CommandMetadata Metadata { get; }
}

/// <summary>
/// Implementation of command context.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public class CommandContext<TCommand> : ICommandContext<TCommand> where TCommand : ICommand
{
    /// <inheritdoc />
    public TCommand Command { get; }

    /// <inheritdoc />
    public CommandMetadata Metadata { get; }

    public CommandContext(TCommand command, CommandMetadata metadata)
    {
        Command = command ?? throw new ArgumentNullException(nameof(command));
        Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
    }
}


