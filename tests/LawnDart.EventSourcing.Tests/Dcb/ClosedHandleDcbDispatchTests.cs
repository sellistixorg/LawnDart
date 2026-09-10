using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventSourcing.Performance;
using LawnDart.Metadata;
using Xunit;

using LawnDart.EventStore;
namespace LawnDart.EventSourcing.Tests.Dcb;

public class ClosedHandleDcbDispatchTests
{
    private static DcbRepository CreateRepository() =>
        new(
            new InMemoryEventStore(enableRegistry: true, logger: NullLogger<InMemoryEventStore>.Instance),
            new DefaultMetadataProvider(),
            new LawnDart.TestUtilities.TestTenantContextProvider("tenant1"),
            Options.Create(new LawnDartOptions()),
            null,
            null,
            NullLogger<DcbRepository>.Instance);

    [Fact]
    public async Task HandleCommandAsync_ClosedHandle_EmitsEvent()
    {
        var repository = CreateRepository();
        var entity = await repository.CreateEntityAsync<ClosedHandleDcbEntity>(["sku:1"]);

        await repository.HandleCommandAsync(entity, new ClosedDcbCommand { Value = "one" });

        Assert.Equal(1, entity.State.EventCount);
        Assert.Contains("one", entity.State.Values);
        Assert.Empty(entity.PendingEvents);
    }

    [Fact]
    public async Task HandleCommandAsync_ClosedHandle_UnknownCommand_Throws()
    {
        var repository = CreateRepository();
        var entity = await repository.CreateEntityAsync<ClosedHandleDcbEntity>(["sku:1"]);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.HandleCommandAsync(entity, new UnknownDcbCommand()));

        Assert.Contains("UnknownDcbCommand", ex.Message);
    }

    [Fact]
    public async Task HandleCommandAsync_GenericOverride_StillWins()
    {
        var repository = CreateRepository();
        var entity = await repository.CreateEntityAsync<GenericOverrideDcbEntity>(["sku:1"]);

        await repository.HandleCommandAsync(entity, new ClosedDcbCommand { Value = "one" });

        Assert.True(entity.GenericCalled);
        Assert.Contains("generic", entity.State.Values);
    }

    [Fact]
    public async Task HandleAsync_GenericOverride_UnknownCommand_DoesNotThrow()
    {
        var entity = new GenericOverrideDcbEntity();

#pragma warning disable CS0618 // Compat path: call obsolete override directly
        await entity.HandleAsync(new UnknownDcbCommand());
#pragma warning restore CS0618

        Assert.True(entity.GenericCalled);
        Assert.Equal(0, entity.State.EventCount);
    }

    [Fact]
    public void OverridesHandleAsync_DetectsGenericAndClosedTypes()
    {
        Assert.False(CompiledCommandApplicator.OverridesHandleAsync(typeof(ClosedHandleDcbEntity)));
        Assert.True(CompiledCommandApplicator.OverridesHandleAsync(typeof(GenericOverrideDcbEntity)));
    }

    private sealed class ClosedDcbState : IState
    {
        public int EventCount { get; set; }
        public List<string> Values { get; } = [];
    }

    private sealed class ClosedHandleDcbEntity : DcbEntity<ClosedDcbState>
    {
        public void Handle(ClosedDcbCommand command) =>
            Emit(new ClosedDcbEvent { Value = command.Value }, Tags.ToArray());

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is ClosedDcbEvent e)
            {
                State.EventCount++;
                State.Values.Add(e.Value);
            }
        }
    }

    private sealed class GenericOverrideDcbEntity : DcbEntity<ClosedDcbState>
    {
        public bool GenericCalled { get; private set; }

        [Obsolete("Declare Handle(TCommand) methods instead.")]
        public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
        {
            GenericCalled = true;
            if (command is ClosedDcbCommand)
                Emit(new ClosedDcbEvent { Value = "generic" }, Tags.ToArray());
            return Task.CompletedTask;
        }

        public void Handle(ClosedDcbCommand command) =>
            Emit(new ClosedDcbEvent { Value = command.Value }, Tags.ToArray());

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is ClosedDcbEvent e)
            {
                State.EventCount++;
                State.Values.Add(e.Value);
            }
        }
    }

    private sealed class ClosedDcbCommand : ICommand
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public string Value { get; init; } = string.Empty;
    }

    private sealed class UnknownDcbCommand : ICommand
    {
        public Guid Id { get; init; } = Guid.NewGuid();
    }
[EventTypeName("closed-handle-dcb-dispatch-tests.closed-dcb-event")]

    private sealed class ClosedDcbEvent : IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
        public string Value { get; init; } = string.Empty;
    }
}
