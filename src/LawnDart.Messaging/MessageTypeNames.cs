using LawnDart.EventStore;

namespace LawnDart.Messaging;

/// <summary>
/// Resolves the message type name stored on the envelope.
/// </summary>
public static class MessageTypeNames
{
    /// <summary>
    /// Returns the <see cref="EventTypeNameAttribute"/> token for <paramref name="messageType"/>.
    /// </summary>
    /// <param name="messageType">The .NET message type.</param>
    /// <returns>The declared event name.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <paramref name="messageType"/> has no <see cref="EventTypeNameAttribute"/>,
    /// or the name is empty. The .NET class name is not used.
    /// </exception>
    public static string Require(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        var name = EventTypeCatalog.TryGetDeclaredName(messageType);
        if (name is null)
        {
            throw new InvalidOperationException(
                $"Message type '{messageType.FullName}' must declare [EventTypeName(\"kebab-token\")]. " +
                "The CLR class name is not used.");
        }

        return name;
    }
}
