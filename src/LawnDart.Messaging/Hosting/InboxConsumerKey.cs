using System.Security.Cryptography;
using System.Text;

namespace LawnDart.Messaging.Hosting;

/// <summary>
/// Builds the inbox key hosted services pass to <see cref="LawnDart.Messaging.IInboxStore"/>.
/// One shared store serves every consumer, so the key is the consumer plus the message id.
/// The consumer segment is a fixed-length hash of the type's full name so the key fits
/// <see cref="MaxKeyLength"/> (the SQL Server inbox column).
/// </summary>
internal static class InboxConsumerKey
{
    /// <summary>Matches <c>SqlServerInboxStore.MaxKeyLength</c> (<c>NVARCHAR(256)</c>).</summary>
    internal const int MaxKeyLength = 256;

    /// <summary>Hex characters taken from SHA-256 of the consumer full name.</summary>
    internal const int ConsumerHashLength = 32;

    internal static string ForReactor(Type reactorType, string messageId)
        => Compose("reactor", reactorType, messageId);

    internal static string ForProcessor(Type processorType, string messageId)
        => Compose("processor", processorType, messageId);

    private static string Compose(string role, Type consumerType, string messageId)
    {
        var name = consumerType.FullName ?? consumerType.Name;
        var consumer = HashSegment(name);
        var key = role + ":" + consumer + ":" + messageId;
        if (key.Length <= MaxKeyLength)
            return key;

        return role + ":" + consumer + ":" + HashSegment(messageId);
    }

    private static string HashSegment(string value)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(hash.AsSpan(0, ConsumerHashLength / 2)).ToLowerInvariant();
    }
}
