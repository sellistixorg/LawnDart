namespace LawnDart.Publishing;

/// <summary>
/// Turns one internal event into the integration event other contexts may read.
/// Found by <c>PublishedEventsBuilder.AddTranslators</c>.
/// </summary>
/// <typeparam name="TInternal">The internal event this context appends.</typeparam>
/// <typeparam name="TPublic">The integration event that goes in the outbox row.</typeparam>
/// <remarks>
/// <para>
/// The translation is pure and synchronous. It runs inside the append, before
/// the database write, and may read only the event and <see cref="PublishContext"/>.
/// If a notice needs more data, put that data on the internal event.
/// </para>
/// <para>
/// One instance is created at startup with the public parameterless constructor,
/// so a translator cannot take services. Return <c>null</c> to publish nothing for
/// this event.
/// </para>
/// </remarks>
public interface IEventTranslator<in TInternal, out TPublic>
    where TInternal : class, IEvent
    where TPublic : class, IEvent
{
    /// <summary>
    /// Builds the integration event for <paramref name="event"/>, or returns
    /// <c>null</c> to publish nothing for it.
    /// </summary>
    TPublic? Translate(TInternal @event, PublishContext context);
}
