using System.Text;
using Amazon.Runtime;
using Amazon.SQS.Model;

namespace LawnDart.Messaging.AwsSqs;

/// <summary>
/// Checks that an SQS or SNS message stays within <see cref="AwsSqsTransportOptions.MaxMessageBytes"/>.
/// The count is the UTF-8 body plus each attribute name, type, and value.
/// </summary>
public static class AwsSqsMessageSize
{
    /// <summary>
    /// Throws <see cref="AwsSqsMessageTooLargeException"/> when <paramref name="body"/> and
    /// <paramref name="attributes"/> are over the limit.
    /// </summary>
    /// <param name="body">Message body.</param>
    /// <param name="attributes">Message attributes. May be empty.</param>
    /// <param name="action">What was about to happen, such as <c>publish</c> or <c>retry</c>.</param>
    public static void Require(
        string? body,
        IReadOnlyDictionary<string, MessageAttributeValue>? attributes,
        string action)
    {
        var size = Measure(body, attributes);
        if (size <= AwsSqsTransportOptions.MaxMessageBytes)
            return;

        throw new AwsSqsMessageTooLargeException(size, action);
    }

    /// <summary>UTF-8 size of the body and attributes.</summary>
    /// <param name="body">Message body.</param>
    /// <param name="attributes">Message attributes. May be null.</param>
    public static int Measure(string? body, IReadOnlyDictionary<string, MessageAttributeValue>? attributes)
    {
        var size = Encoding.UTF8.GetByteCount(body ?? string.Empty);
        if (attributes is null)
            return size;

        foreach (var pair in attributes)
        {
            size += Encoding.UTF8.GetByteCount(pair.Key);
            if (pair.Value is null)
                continue;
            if (!string.IsNullOrEmpty(pair.Value.DataType))
                size += Encoding.UTF8.GetByteCount(pair.Value.DataType);
            if (!string.IsNullOrEmpty(pair.Value.StringValue))
                size += Encoding.UTF8.GetByteCount(pair.Value.StringValue);
        }

        return size;
    }

    /// <summary>
    /// <c>true</c> when <paramref name="exception"/> is a size rejection from this transport or from AWS.
    /// </summary>
    /// <param name="exception">The failure.</param>
    public static bool IsTooLarge(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is AwsSqsMessageTooLargeException)
                return true;

            if (current is not AmazonServiceException service)
                continue;

            var text = service.Message ?? string.Empty;
            if (text.Contains("too large", StringComparison.OrdinalIgnoreCase) ||
                text.Contains("Message must be shorter", StringComparison.OrdinalIgnoreCase) ||
                text.Contains("256 KB", StringComparison.OrdinalIgnoreCase) ||
                text.Contains("262144", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}

/// <summary>
/// The message body and attributes are larger than SQS and SNS accept.
/// </summary>
public sealed class AwsSqsMessageTooLargeException : InvalidOperationException
{
    /// <summary>
    /// Creates the exception.
    /// </summary>
    /// <param name="size">Measured size in bytes.</param>
    /// <param name="action">What was about to happen.</param>
    public AwsSqsMessageTooLargeException(int size, string action)
        : base(
            $"The SQS message is {size} bytes. The limit is {AwsSqsTransportOptions.MaxMessageBytes} bytes, including attributes. " +
            $"The {action} was not sent.")
    {
        Size = size;
        Action = action;
    }

    /// <summary>Measured size in bytes.</summary>
    public int Size { get; }

    /// <summary>What was about to happen.</summary>
    public string Action { get; }
}
