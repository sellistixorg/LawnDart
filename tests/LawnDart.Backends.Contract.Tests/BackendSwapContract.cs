using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using LawnDart.Aggregates;
using LawnDart.Backends.Contract.Tests.SwapApp;
using LawnDart.EventStore;
using LawnDart.Messaging;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Storage;
using LawnDart.Snapshots;
using LawnDart.Testing;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Shared assertions for the InMemory → SQL Server DI swap. One body, two hosts.
/// Covers command dispatch, event persistence, aggregate reload, projection
/// materialisation, and read-back. Outbox and subscriptions are not in this contract.
/// </summary>
internal static class BackendSwapContract
{
    public static async Task DispatchPersistReloadProjectReadBackAsync(SwapHost host)
    {
        var dispatcher = host.Services.GetRequiredService<ICommandDispatcher>();
        var store = host.Services.GetRequiredKeyedService<IEventStore>(SwapHost.ContextName);
        var repository = host.Services.GetRequiredService<IAggregateRepository>();
        var snapshots = host.Services.GetRequiredService<ISnapshotStore>();
        var views = host.Services.GetRequiredKeyedService<IViewStore>(SwapHost.ContextName);

        var studentId = Guid.NewGuid();
        var streamId = $"Student:{studentId}";
        var command = new EnrollStudent(Guid.NewGuid(), studentId, "Ada Lovelace");

        await dispatcher.DispatchAsync(command, new MessageContext());

        var events = await store.ReadStreamAsync(streamId);
        Assert.Single(events);
        var enrolled = Assert.IsType<StudentEnrolled>(events[0].Event);
        Assert.Equal("Ada Lovelace", enrolled.Name);
        Assert.Equal(studentId, enrolled.StudentId);

        await WaitForAsync.UntilAsync(async () =>
        {
            var (_, info) = await snapshots.LoadSnapshotAsync<StudentState>(streamId);
            return info is { Version: >= 1 };
        }, timeout: TimeSpan.FromSeconds(10));

        var reloaded = await repository.GetAsync<Student>(studentId);
        Assert.NotNull(reloaded);
        Assert.True(reloaded!.State.Enrolled);
        Assert.Equal("Ada Lovelace", reloaded.State.Name);

        string? viewJson = null;
        await WaitForAsync.UntilAsync(async () =>
        {
            viewJson = await views.GetViewAsync(SwapHost.RosterStorageKey, streamId);
            return viewJson is not null && viewJson.Contains("Ada Lovelace", StringComparison.Ordinal);
        }, timeout: TimeSpan.FromSeconds(10));

        var view = JsonSerializer.Deserialize<StudentRosterView>(viewJson!, ProjectionViewJson.Read);
        Assert.NotNull(view);
        Assert.True(view!.Enrolled);
        Assert.Equal("Ada Lovelace", view.Name);
    }
}
