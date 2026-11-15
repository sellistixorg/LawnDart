namespace LawnDart.Messaging;

/// <summary>
/// What startup does when a declared published type has no listener.
/// </summary>
public enum MissingListenerBehavior
{
    /// <summary>
    /// Log a warning and continue. The host still starts. This is the default.
    /// </summary>
    Warn = 0,

    /// <summary>
    /// Stop host startup when the check finds a declared type with no listener.
    /// When the check cannot tell, it logs and the host still starts.
    /// </summary>
    Throw = 1,
}
