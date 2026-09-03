namespace LawnDart.Serialization;

/// <summary>
/// Specifies the serialization order for properties in events and commands.
/// This attribute is CRITICAL for wire protocol compatibility in binary serialization formats (Protobuf, Avro).
/// </summary>
/// <remarks>
/// <para><strong>Why PropertyOrder Matters:</strong></para>
/// <list type="bullet">
///   <item>Binary serialization formats (Protobuf, Avro) rely on field order for schema evolution</item>
///   <item>Reordering fields or changing field numbers BREAKS backward compatibility</item>
///   <item>Provides a single source of truth for field ordering across all serializers</item>
///   <item>Educational: Makes developers explicitly aware of serialization order requirements</item>
/// </list>
/// 
/// <para><strong>Critical Rules:</strong></para>
/// <list type="number">
///   <item>Field numbers must be UNIQUE within a type</item>
///   <item>Field numbers must NEVER change once deployed to production</item>
///   <item>Field numbers must NEVER be reused (even for deleted fields)</item>
///   <item>Start at 1 and use sequential ordering for optimal encoding</item>
///   <item>Reserve gaps (e.g., 1-15, then 100-115) for future fields if needed</item>
/// </list>
/// 
/// <para><strong>Usage:</strong></para>
/// <code>
/// public record OrderCreated(
///     [PropertyOrder(1)] Guid Id,
///     [PropertyOrder(2)] DateTime Timestamp,
///     [PropertyOrder(3)] string OrderNumber,
///     [PropertyOrder(4)] decimal Amount) : IEvent;
/// </code>
/// 
/// <para><strong>Schema Evolution:</strong></para>
/// <para>When adding new fields, always use NEW field numbers:</para>
/// <code>
/// // Version 2 - Adding CustomerName
/// public record OrderCreated(
///     [PropertyOrder(1)] Guid Id,
///     [PropertyOrder(2)] DateTime Timestamp,
///     [PropertyOrder(3)] string OrderNumber,
///     [PropertyOrder(4)] decimal Amount,
///     [PropertyOrder(5)] string CustomerName) : IEvent;  // New field = new number
/// </code>
/// 
/// <para><strong>Compatibility:</strong></para>
/// <para>Drives both Protobuf (protobuf-net RuntimeTypeModel) and Avro (schema generation),
/// ensuring serialized data is compatible with standard Protobuf and Avro tools.</para>
/// </remarks>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false)]
public sealed class PropertyOrderAttribute : Attribute
{
    /// <summary>
    /// Gets the serialization order number for this property.
    /// Must be unique, positive, and never changed after deployment.
    /// </summary>
    public int Order { get; }
    
    /// <summary>
    /// Initializes a new instance of PropertyOrderAttribute.
    /// </summary>
    /// <param name="order">The serialization order (must be greater than 0)</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when order is less than or equal to 0</exception>
    public PropertyOrderAttribute(int order)
    {
        if (order <= 0)
            throw new ArgumentOutOfRangeException(nameof(order), 
                "Property order must be greater than 0. Field numbering starts at 1.");
        
        Order = order;
    }
}
