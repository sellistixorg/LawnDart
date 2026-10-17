using Testcontainers.MsSql;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>Shared SQL Server container for Matrix A flush/checkpoint e2e (one container per collection).</summary>
public sealed class MsSqlMatrixAFixture : IAsyncLifetime
{
    private MsSqlContainer? _container;

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025)
                .WithPassword("MatrixATest1!")
                .Build());
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }
}

[CollectionDefinition("SqlFlushMatrixA")]
public sealed class SqlFlushMatrixACollection : ICollectionFixture<MsSqlMatrixAFixture>;
