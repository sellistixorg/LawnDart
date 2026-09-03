namespace LawnDart.Patterns.StateTransformation;

/// <summary>
/// Transforms one state type into another (State → State pattern).
/// Interface stub — no implementation is provided yet.
/// </summary>
/// <typeparam name="TInput">The source state type.</typeparam>
/// <typeparam name="TOutput">The target state type.</typeparam>
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
