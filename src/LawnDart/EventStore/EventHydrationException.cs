namespace LawnDart.EventStore;

/// <summary>
/// Typed-session fail-closed read. Log and raw copy paths do not throw these.
/// There is no runtime override to skip the event; deploy the missing type
/// or upcaster. Subscriptions do not advance the typed cursor past the frame.
/// </summary>
public abstract class EventHydrationException : InvalidOperationException
{
    /// <summary>Family token on the recorded frame, when known.</summary>
    public string? FamilyToken { get; }

    /// <summary>Stored schema version on the recorded frame, when known.</summary>
    public int? SchemaVersion { get; }

    private protected EventHydrationException(
        string message,
        string? familyToken = null,
        int? schemaVersion = null,
        Exception? innerException = null)
        : base(message, innerException)
    {
        FamilyToken = familyToken;
        SchemaVersion = schemaVersion;
    }
}

/// <summary>
/// The log contains a <c>SchemaVersion</c> newer than this process's current
/// type for a known family.
/// </summary>
public sealed class EventSchemaTooNewException : EventHydrationException
{
    /// <summary>This process's current schema version for the family.</summary>
    public int ProcessCurrentVersion { get; }

    /// <summary>Creates the exception.</summary>
    public EventSchemaTooNewException(
        string familyToken,
        int storedVersion,
        int processCurrentVersion)
        : base(
            $"Event family '{familyToken}' has SchemaVersion {storedVersion}, " +
            $"which is newer than this process's current version {processCurrentVersion}. " +
            "Deploy a binary that understands this version. There is no skip override.",
            familyToken,
            storedVersion)
    {
        ProcessCurrentVersion = processCurrentVersion;
    }
}

/// <summary>
/// Typed read of a family token that is not in this process's catalog.
/// </summary>
public sealed class UnknownEventFamilyException : EventHydrationException
{
    /// <summary>Creates the exception.</summary>
    public UnknownEventFamilyException(string familyToken)
        : base(
            $"Unknown event family '{familyToken}'. " +
            "Register the type with WithEventTypes or read the frame through IEventLog. " +
            "There is no skip override.",
            familyToken)
    {
    }
}

/// <summary>
/// Known family, stored version is not newer than current, but that version's
/// CLR type is not in the catalog.
/// </summary>
public sealed class EventSchemaNotInCatalogException : EventHydrationException
{
    /// <summary>Creates the exception.</summary>
    public EventSchemaNotInCatalogException(string familyToken, int schemaVersion)
        : base(
            $"Event family '{familyToken}' SchemaVersion {schemaVersion} is not in this process's catalog. " +
            "Register the historical CLR type. There is no skip override.",
            familyToken,
            schemaVersion)
    {
    }
}

/// <summary>
/// Stored payload content-type does not match this session's codec.
/// </summary>
public sealed class EventContentTypeMismatchException : EventHydrationException
{
    /// <summary>Content-type on the recorded frame.</summary>
    public string StoredContentType { get; }

    /// <summary>Content-type of the session codec.</summary>
    public string SessionContentType { get; }

    /// <summary>Creates the exception.</summary>
    public EventContentTypeMismatchException(string storedContentType, string sessionContentType)
        : base(
            $"Event stored with ContentType '{storedContentType}' but current serializer uses '{sessionContentType}'.")
    {
        StoredContentType = storedContentType;
        SessionContentType = sessionContentType;
    }
}

/// <summary>
/// Payload bytes did not deserialize to the stored version's CLR type.
/// </summary>
public sealed class EventPayloadException : EventHydrationException
{
    /// <summary>Creates the exception.</summary>
    public EventPayloadException(string familyToken, int schemaVersion, string message, Exception? innerException = null)
        : base(message, familyToken, schemaVersion, innerException)
    {
    }
}

/// <summary>
/// Historical type is in the catalog but no upcaster reaches the current type.
/// Thrown when upcast-on-read runs (<c>SCH-06</c>). There is no skip override;
/// deploy the missing upcaster.
/// </summary>
public sealed class MissingEventUpcasterException : EventHydrationException
{
    /// <summary>Stored version that needs an upcaster.</summary>
    public int FromVersion { get; }

    /// <summary>This process's current version.</summary>
    public int ToVersion { get; }

    /// <summary>Creates the exception.</summary>
    public MissingEventUpcasterException(string familyToken, int fromVersion, int toVersion)
        : base(
            $"No upcaster from SchemaVersion {fromVersion} to {toVersion} for family '{familyToken}'. " +
            "Deploy the missing upcaster. There is no skip override.",
            familyToken,
            fromVersion)
    {
        FromVersion = fromVersion;
        ToVersion = toVersion;
    }
}
