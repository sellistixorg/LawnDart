namespace LawnDart.EventStore;

/// <summary>
/// Utility for computing partition hashes for event store queries.
/// Uses FNV-1a hash algorithm for deterministic, process-independent hashing.
/// </summary>
public static class PartitionHashUtility
{
    /// <summary>
    /// Compute a deterministic hash code that's consistent across all processes.
    /// Based on FNV-1a hash algorithm.
    /// </summary>
    public static int GetDeterministicHashCode(string str)
    {
        if (string.IsNullOrEmpty(str))
            throw new ArgumentNullException(nameof(str));
            
        unchecked
        {
            const uint FnvPrime = 16777619;
            uint hash = 2166136261; // FNV offset basis
            
            foreach (char c in str)
            {
                hash ^= c;
                hash *= FnvPrime;
            }
            
            // Ensure result is positive and fits in int range
            return (int)(hash & 0x7FFFFFFF);
        }
    }
}
