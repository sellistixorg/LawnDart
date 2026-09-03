using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using LawnDart.Messaging.Hosting;
using LawnDart.Patterns.EventProcessing;
using LawnDart.Patterns.Reaction;
using LawnDart.Patterns.TaskProcessing;

namespace LawnDart.Messaging;

/// <summary>
/// DI registration helpers for the EDA messaging infrastructure.
/// </summary>
public static class MessagingExtensions
{
    /// <summary>
    /// Registers core messaging options and the <see cref="IInboxStore"/> placeholder.
    /// Call this once per application, then call a transport-specific extension
    /// (e.g. <c>AddInMemoryMessaging()</c> or <c>AddServiceBusMessaging()</c>).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional options configurator.</param>
    public static IServiceCollection AddMessaging(
        this IServiceCollection services,
        Action<MessagingOptions>? configure = null)
    {
        if (configure is not null)
            services.Configure(configure);
        else
            services.Configure<MessagingOptions>(_ => { });

        return services;
    }

    /// <summary>
    /// Registers a reactor and its hosting infrastructure.
    /// The reactor is invoked for each <typeparamref name="TEvent"/> message received
    /// from the registered <see cref="IMessageTransport"/>.
    /// </summary>
    /// <typeparam name="TReactor">The reactor implementation.</typeparam>
    /// <typeparam name="TEvent">The event type the reactor handles.</typeparam>
    public static IServiceCollection AddReactor<TReactor, TEvent>(this IServiceCollection services)
        where TReactor : class, IReactor<TEvent>
        where TEvent : class, IEvent
    {
        services.TryAddTransient<TReactor>();
        services.AddHostedService<ReactorHostedService<TReactor, TEvent>>();
        return services;
    }

    /// <summary>
    /// Registers an event processor and its hosting infrastructure.
    /// The processor is invoked for each <typeparamref name="TEvent"/> message received
    /// from the registered <see cref="IMessageTransport"/>.
    /// </summary>
    /// <typeparam name="TProcessor">The event processor implementation.</typeparam>
    /// <typeparam name="TEvent">The input event type the processor handles.</typeparam>
    public static IServiceCollection AddEventProcessor<TProcessor, TEvent>(this IServiceCollection services)
        where TProcessor : class, IEventProcessor<TEvent>
        where TEvent : class, IEvent
    {
        services.TryAddTransient<TProcessor>();
        services.AddHostedService<EventProcessorHostedService<TProcessor, TEvent>>();
        return services;
    }

    /// <summary>
    /// Registers a task processor and its polling hosted service with default options.
    /// </summary>
    /// <typeparam name="TProcessor">The task processor implementation.</typeparam>
    public static IServiceCollection AddTaskProcessor<TProcessor>(this IServiceCollection services)
        where TProcessor : class, ITaskProcessor
    {
        services.TryAddTransient<TProcessor>();
        services.TryAddSingleton(Options.Create(new TaskProcessorOptions()));
        services.AddHostedService<TaskProcessorHostedService<TProcessor>>();
        return services;
    }

    /// <summary>
    /// Registers a task processor and its polling hosted service with custom options.
    /// </summary>
    /// <typeparam name="TProcessor">The task processor implementation.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configurator for polling interval and error backoff.</param>
    public static IServiceCollection AddTaskProcessor<TProcessor>(
        this IServiceCollection services,
        Action<TaskProcessorOptions> configure)
        where TProcessor : class, ITaskProcessor
    {
        services.TryAddTransient<TProcessor>();
        services.Configure(configure);
        services.AddHostedService<TaskProcessorHostedService<TProcessor>>();
        return services;
    }

    /// <summary>
    /// Registers a custom <see cref="ICommandDispatcher"/> implementation that routes commands
    /// emitted by reactors and task processors to their handlers.
    /// </summary>
    /// <typeparam name="TDispatcher">The dispatcher implementation.</typeparam>
    public static IServiceCollection AddCommandDispatcher<TDispatcher>(this IServiceCollection services)
        where TDispatcher : class, ICommandDispatcher
    {
        services.AddSingleton<ICommandDispatcher, TDispatcher>();
        return services;
    }
}
