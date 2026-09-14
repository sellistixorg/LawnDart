using Testcontainers.MsSql;

namespace LawnDart.Backends.Contract.Tests;

public sealed class SqlServerContractFixture : IAsyncLifetime
{
    private MsSqlContainer? _container;

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder(MsSqlTestImage.Server2022)
            .WithPassword("Test123!")
            .Build();
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }
}

[CollectionDefinition("SqlServerContract")]
public sealed class SqlServerContractCollection : ICollectionFixture<SqlServerContractFixture>;
