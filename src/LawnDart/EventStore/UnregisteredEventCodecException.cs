namespace LawnDart.EventStore;

/// <summary>
/// <see cref="EventCodec.IdFor"/> was called with a MIME that is not built-in
/// and was not passed to <see cref="EventCodec.Register"/>.
/// </summary>
public sealed class UnregisteredEventCodecException : ArgumentException
{
    /// <summary>The MIME that was not in the intern table.</summary>
    public string Mime { get; }

    /// <summary>Creates the exception. The message names <see cref="EventCodec.Register"/>.</summary>
    public UnregisteredEventCodecException(string mime)
        : base(
            $"MIME '{mime}' is not a registered event codec. " +
            $"Call {nameof(EventCodec)}.{nameof(EventCodec.Register)} with an id in " +
            $"{EventCodec.UserRangeStart}–{EventCodec.UserRangeEnd}.",
            nameof(mime))
    {
        Mime = mime;
    }
}
