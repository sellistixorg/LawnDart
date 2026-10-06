using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.Outbox;

public sealed class SqlServerOutboxResetFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder(MsSqlTestImage.Server2022).Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}
