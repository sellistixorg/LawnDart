using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using LawnDart;
using LawnDart.AspNetCore.Tests.DefaultContext;
using LawnDart.EventSourcing;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.AspNetCore.Tests.Integration
{
    /// <summary>
    /// HTTP commands against a bounded context with no hand-written keyed→unkeyed bridges.
    /// </summary>
    public class BoundedContextHttpCommandTests
    {
        [Fact]
        public async Task PostCommand_DefaultContext_ResolvesUnkeyedStoreWithoutManualBridge()
        {
            using var host = BuildHost(services =>
            {
                services.AddLawnDart(o =>
                {
                    o.RequireTenantId = false;
                    o.EnableAuthorization = false;
                });
                services.AddBoundedContext("default").UseInMemory();
                services.AddLawnDartHttpCommands(typeof(AppendNoteCommand).Assembly);
            }, endpoints => endpoints.MapLawnDartCommands());

            var client = host.GetTestClient();
            var id = Guid.NewGuid();
            var response = await client.PostAsJsonAsync(
                "/api/default-context/append-note",
                new { Id = id, Text = "hello-default" });

            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

            var store = host.Services.GetRequiredService<IEventStore>();
            var events = await store.ReadStreamAsync($"note-{id:N}");
            Assert.Single(events);
        }

        [Fact]
        public async Task PostCommand_InboundTraceparent_AppearsOnEventMetadataTraceId()
        {
            using var listener = EnableAllActivities();
            using var host = BuildHost(services =>
            {
                services.AddLawnDart(o =>
                {
                    o.RequireTenantId = false;
                    o.EnableAuthorization = false;
                });
                services.AddBoundedContext("default").UseInMemory();
                services.AddLawnDartHttpCommands(typeof(TraceNoteCommand).Assembly);
            }, endpoints => endpoints.MapLawnDartCommands());

            var client = host.GetTestClient();
            var id = Guid.NewGuid();
            using var request = new HttpRequestMessage(HttpMethod.Post, "/api/default-context/trace-note")
            {
                Content = JsonContent.Create(new { Id = id, Text = "traced" })
            };
            request.Headers.TryAddWithoutValidation(
                "traceparent",
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");

            var response = await client.SendAsync(request);
            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

            var store = host.Services.GetRequiredService<IEventStore>();
            var events = await store.ReadStreamAsync($"trace-note-{id:N}");
            var evt = Assert.Single(events);
            Assert.Equal("0af7651916cd43dd8448eb211c80319c", evt.Metadata.TraceId);
            Assert.Equal("http-default-trace-note-appended", evt.Metadata.SchemaName);
            Assert.NotEqual(typeof(TraceNoteAppended).FullName, evt.Metadata.SchemaName);
        }

        [Fact]
        public async Task PostCommand_NamedContext_ResolvesKeyedHandlerWithoutUnkeyedStore()
        {
            using var host = BuildHost(services =>
            {
                services.AddLawnDart(o =>
                {
                    o.RequireTenantId = false;
                    o.EnableAuthorization = false;
                });
                services.AddBoundedContext("ordering")
                    .UseInMemory()
                    .WithCommandHandlers([typeof(OrderingContext.AppendNoteCommandHandler)]);
                services.AddLawnDartHttpCommands("ordering", typeof(OrderingContext.AppendNoteCommandHandler).Assembly);
            }, endpoints => endpoints.MapLawnDartCommands("ordering"));

            Assert.Null(host.Services.GetService<IEventStore>());
            Assert.NotNull(host.Services.GetRequiredKeyedService<IEventStore>("ordering"));

            var client = host.GetTestClient();
            var id = Guid.NewGuid();
            var response = await client.PostAsJsonAsync(
                "/api/ordering-context/append-note",
                new { Id = id, Text = "hello-ordering" });

            Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

            var store = host.Services.GetRequiredKeyedService<IEventStore>("ordering");
            var events = await store.ReadStreamAsync($"note-{id:N}");
            Assert.Single(events);
        }

        private static IHost BuildHost(
            Action<IServiceCollection> configureServices,
            Action<IEndpointRouteBuilder> configureEndpoints)
        {
            var host = new HostBuilder()
                .ConfigureWebHost(webHost =>
                {
                    webHost.UseTestServer();
                    webHost.UseEnvironment("Test");
                    webHost.ConfigureServices(configureServices);
                    webHost.Configure(app =>
                    {
                        app.UseRouting();
                        app.UseEndpoints(configureEndpoints);
                    });
                })
                .Build();

            host.Start();
            return host;
        }

        private static ActivityListener EnableAllActivities()
        {
            var listener = new ActivityListener
            {
                ShouldListenTo = static _ => true,
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(listener);
            return listener;
        }
    }
}

namespace LawnDart.AspNetCore.Tests.DefaultContext
{
    public record AppendNoteCommand(Guid Id, string Text) : ICommand;

    [EventTypeName("http-default-note-appended")]
    public sealed record NoteAppended(Guid Id, DateTime Timestamp, string Text) : IEvent;

    public sealed class AppendNoteCommandHandler(IEventStore store) : ICommandHandler<AppendNoteCommand>
    {
        public async Task HandleAsync(AppendNoteCommand command, CancellationToken cancellationToken = default)
        {
            await store.AppendAsync(
                $"note-{command.Id:N}",
                [new NoteAppended(Guid.NewGuid(), DateTime.UtcNow, command.Text)],
                cancellationToken: cancellationToken);
        }
    }

    public record TraceNoteCommand(Guid Id, string Text) : ICommand;

    [EventTypeName("http-default-trace-note-appended")]
    public sealed record TraceNoteAppended(Guid Id, DateTime Timestamp, string Text) : IEvent;

    public sealed class TraceNoteCommandHandler(IEventStore store, IMetadataProvider metadata)
        : ICommandHandler<TraceNoteCommand>
    {
        public async Task HandleAsync(TraceNoteCommand command, CancellationToken cancellationToken = default)
        {
            var commandMetadata = metadata.CaptureCommandMetadata();
            var @event = new TraceNoteAppended(Guid.NewGuid(), DateTime.UtcNow, command.Text);
            var envelope = metadata.EnrichEventMetadata(new EventMetadata(), commandMetadata, @event);
            await store.AppendAsync(
                $"trace-note-{command.Id:N}",
                [@event],
                metadata: envelope,
                cancellationToken: cancellationToken);
        }
    }
}

namespace LawnDart.AspNetCore.Tests.OrderingContext
{
    public record AppendNoteCommand(Guid Id, string Text) : ICommand;

    [EventTypeName("http-ordering-note-appended")]
    public sealed record NoteAppended(Guid Id, DateTime Timestamp, string Text) : IEvent;

    public sealed class AppendNoteCommandHandler(IEventStore store) : ICommandHandler<AppendNoteCommand>
    {
        public async Task HandleAsync(AppendNoteCommand command, CancellationToken cancellationToken = default)
        {
            await store.AppendAsync(
                $"note-{command.Id:N}",
                [new NoteAppended(Guid.NewGuid(), DateTime.UtcNow, command.Text)],
                cancellationToken: cancellationToken);
        }
    }
}
