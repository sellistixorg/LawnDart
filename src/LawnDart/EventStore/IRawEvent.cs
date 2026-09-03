namespace LawnDart.EventStore;

/// <summary>
/// Marker interface for events that carry a pre-serialized payload and a type name
/// but do not have their concrete CLR type loaded in the current process.
/// <para>
/// Implementations include <c>OpaqueEvent</c> (used on the gRPC server when receiving
/// events from a remote client) and <c>UnknownEvent</c> (used when reading events whose
/// type is not registered in the current process).
/// </para>
/// <para>
/// Consumers such as <c>EventProtoMapper</c> should detect this interface and return
/// <see cref="RawPayload"/> bytes directly without attempting to re-serialize.
/// </para>
/// </summary>
public interface IRawEvent : LawnDart.IEvent
{
    /// <summary>The stable event type name as stored on disk.</summary>
    string TypeName { get; }

    /// <summary>The raw serialized payload bytes.</summary>
    byte[] RawPayload { get; }
}
