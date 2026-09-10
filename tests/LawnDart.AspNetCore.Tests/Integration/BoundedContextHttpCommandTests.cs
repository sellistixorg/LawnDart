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
