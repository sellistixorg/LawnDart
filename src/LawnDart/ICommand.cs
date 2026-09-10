namespace LawnDart;

/// <summary>
/// Represents a command - an intent to perform an action (future intent).
/// </summary>
public interface ICommand
{
    /// <summary>
    /// Unique identifier for the command.
    /// </summary>
    Guid Id { get; }
}


