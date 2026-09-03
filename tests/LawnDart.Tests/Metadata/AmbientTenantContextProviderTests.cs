using LawnDart.Metadata;

namespace LawnDart.Tests.Metadata;

/// <summary>
/// Tests for AmbientTenantContextProvider (1.T4, 1.T5).
/// </summary>
public class AmbientTenantContextProviderTests
{
    // ─────────── 1.T4: scope behaviour ───────────

    [Fact]
    public void GetTenantId_BeforeSetTenant_ReturnsNull()
    {
        var provider = new AmbientTenantContextProvider();
        Assert.Null(provider.GetTenantId());
    }

    [Fact]
    public void GetTenantId_InsideScope_ReturnsSetValue()
    {
        var provider = new AmbientTenantContextProvider();
        using (provider.SetTenant("acme"))
        {
            Assert.Equal("acme", provider.GetTenantId());
        }
    }

    [Fact]
    public void GetTenantId_AfterScopeDisposed_ReturnsNull()
    {
        var provider = new AmbientTenantContextProvider();
        using (provider.SetTenant("acme")) { /* no-op inside scope */ }
        Assert.Null(provider.GetTenantId());
    }

    [Fact]
    public void GetTenantId_NestedScopes_RestoresOuterValueOnInnerDisposal()
    {
        var provider = new AmbientTenantContextProvider();
        using (provider.SetTenant("outer"))
        {
            Assert.Equal("outer", provider.GetTenantId());
            using (provider.SetTenant("inner"))
            {
                Assert.Equal("inner", provider.GetTenantId());
            }
            Assert.Equal("outer", provider.GetTenantId());
        }
        Assert.Null(provider.GetTenantId());
    }

    [Fact]
    public void GetTenantIdRequired_WithinScope_ReturnsValue()
    {
        var provider = new AmbientTenantContextProvider();
        using (provider.SetTenant("shop"))
        {
            Assert.Equal("shop", provider.GetTenantIdRequired());
        }
    }

    [Fact]
    public void GetTenantIdRequired_OutsideScope_Throws()
    {
        var provider = new AmbientTenantContextProvider();
        var ex = Assert.Throws<InvalidOperationException>(() => provider.GetTenantIdRequired());
        Assert.Contains("AmbientTenantContextProvider", ex.Message);
    }

    [Fact]
    public void SetTenant_CanBeDisposedTwice_NoException()
    {
        var provider = new AmbientTenantContextProvider();
        var scope = provider.SetTenant("acme");
        scope.Dispose();
        scope.Dispose(); // second dispose should not throw
    }

    // ─────────── 1.T5: AsyncLocal isolation ───────────

    [Fact]
    public async Task SetTenant_AsyncLocalIsolation_DoesNotBleedIntoSiblingTask()
    {
        var provider = new AmbientTenantContextProvider();

        // Barrier to synchronise both tasks at the point where tenant context is set
        var barrier = new SemaphoreSlim(0, 1);

        string? siblingReading = "NOT_READ_YET";

        var siblingTask = Task.Run(async () =>
        {
            // Sibling waits until the main context has set its tenant
            await barrier.WaitAsync();
            siblingReading = provider.GetTenantId();
        });

        using (provider.SetTenant("task-tenant"))
        {
            barrier.Release(); // let sibling run now
            await Task.Delay(20); // give sibling time to read
        }

        await siblingTask;

        // The sibling task has its own AsyncLocal slot — it must NOT see "task-tenant"
        Assert.Null(siblingReading);
    }

    [Fact]
    public async Task SetTenant_PropagatesIntoChildContinuations()
    {
        var provider = new AmbientTenantContextProvider();
        string? readInContinuation = null;

        using (provider.SetTenant("continuation-tenant"))
        {
            await Task.Yield();
            readInContinuation = provider.GetTenantId();
        }

        Assert.Equal("continuation-tenant", readInContinuation);
    }
}
