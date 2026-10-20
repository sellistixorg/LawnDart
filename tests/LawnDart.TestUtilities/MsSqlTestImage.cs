namespace LawnDart.TestUtilities;

/// <summary>
/// Pinned SQL Server image for Testcontainers.
/// Do not use floating tags such as <c>2025-latest</c>.
/// SQL Server 2025 is required for the VECTOR type.
/// The Ubuntu 22.04 base is used because the 24.04 base crashes more often
/// on startup on GitHub Actions (microsoft/mssql-docker#974).
/// </summary>
public static class MsSqlTestImage
{
    /// <summary>
    /// SQL Server 2025 CU9 GDR1 on Ubuntu 22.04. Used by every Testcontainers fixture.
    /// </summary>
    public const string Server2025 = "mcr.microsoft.com/mssql/server:2025-CU9-GDR1-ubuntu-22.04";
}

/// <summary>
/// Pinned Postgres images for Testcontainers. Do not use a floating tag.
/// </summary>
public static class PostgresTestImage
{
    /// <summary>Postgres 18. Used by every pull request.</summary>
    public const string Version18 = "postgres:18.1";

    /// <summary>Oldest supported Postgres. Runs when the path filter matches.</summary>
    public const string Version15 = "postgres:15.15";

    /// <summary>
    /// Image for this process. <c>LAWNDART_POSTGRES_IMAGE</c> overrides the default.
    /// </summary>
    public static string Current =>
        Environment.GetEnvironmentVariable("LAWNDART_POSTGRES_IMAGE") is { Length: > 0 } image
            ? image
            : Version18;
}
