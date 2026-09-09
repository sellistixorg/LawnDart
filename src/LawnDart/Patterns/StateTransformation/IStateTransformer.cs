using System.Diagnostics.CodeAnalysis;

namespace LawnDart.Patterns.StateTransformation;

/// <summary>
/// Transforms one state type into another (State → State pattern).
/// </summary>
/// <remarks>
/// Planned cell (🔧). No host in this version. Implementing this
/// interface does not register it with DI or cause the runtime to
/// invoke it.
/// </remarks>
/// <typeparam name="TInput">The source state type.</typeparam>
/// <typeparam name="TOutput">The target state type.</typeparam>
[Experimental("LAWNDART002")]
public interface IStateTransformer<in TInput, TOutput>
    where TInput : IState
    where TOutput : IState
{
    /// <summary>
    /// Transforms the input state into the output state.
    /// </summary>
    /// <param name="input">Source state.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The transformed output state.</returns>
    Task<TOutput> TransformAsync(TInput input, CancellationToken cancellationToken = default);
}
