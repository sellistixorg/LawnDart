using System.Text;
using DotNet.Testcontainers.Containers;
using Testcontainers.MsSql;

namespace LawnDart.TestUtilities;

/// <summary>
/// Starts one SQL Server Testcontainer and retries the known SQL Server 2025 startup crash.
/// Each attempt builds a new container. A failed container is disposed before the next attempt.
/// Callers await this method, so containers still start one at a time.
/// A retry writes <c>MsSqlContainerStartup retry</c> to the test console and to process standard error.
/// </summary>
public static class MsSqlContainerStartup
{
    private const int MaxAttempts = 3;

    /// <summary>
    /// Builds and starts a SQL Server container.
    /// When the container fails to start, or exits during startup, disposes it and tries again.
    /// Three attempts total. A cancelled start is not retried.
    /// </summary>
    /// <param name="build">Creates an unstarted container. Called once per attempt.</param>
    /// <param name="cancellationToken">Cancels startup. Cancellation is not retried.</param>
    /// <returns>The container that started and was still running.</returns>
    public static async Task<MsSqlContainer> StartAsync(
        Func<MsSqlContainer> build,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(build);

        for (var attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var container = build();
            try
            {
                await container.StartAsync(cancellationToken).ConfigureAwait(false);
                if (container.State == TestcontainersStates.Exited)
                    throw new InvalidOperationException("SQL Server container exited during startup.");

                return container;
            }
            catch (Exception)
            {
                await DisposeQuietlyAsync(container).ConfigureAwait(false);
                if (cancellationToken.IsCancellationRequested || attempt >= MaxAttempts)
                    throw;

                WriteRetry(attempt);
            }
        }
    }

    private static void WriteRetry(int attempt)
    {
        var message =
            $"MsSqlContainerStartup retry {attempt}/{MaxAttempts}: container failed to start or exited during startup.";
        Console.WriteLine(message);
        try
        {
            using var stderr = new StreamWriter(
                Console.OpenStandardError(),
                Encoding.UTF8,
                bufferSize: 256,
                leaveOpen: true)
            {
                AutoFlush = true
            };
            stderr.WriteLine(message);
        }
        catch (IOException)
        {
            // The console line is enough when standard error cannot be opened.
        }
    }

    private static async Task DisposeQuietlyAsync(MsSqlContainer container)
    {
        try
        {
            await container.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The container is already being discarded. The next attempt builds a new one.
        }
    }
}
