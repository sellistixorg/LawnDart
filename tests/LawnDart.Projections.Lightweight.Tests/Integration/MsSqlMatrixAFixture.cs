using Testcontainers.MsSql;

namespace LawnDart.Projections.Lightweight.Tests.Integration;

/// <summary>Shared SQL Server container for Matrix A flush/checkpoint e2e (one container per collection).</summary>
public sealed class MsSqlMatrixAFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .WithPassword("MatrixATest1!")
        .Build();

    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();
}

[CollectionDefinition("SqlFlushMatrixA")]
public sealed class SqlFlushMatrixACollection : ICollectionFixture<MsSqlMatrixAFixture>;
