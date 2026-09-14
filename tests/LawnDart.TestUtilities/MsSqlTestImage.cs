namespace LawnDart.TestUtilities;

/// <summary>
/// Pinned SQL Server image for Testcontainers. Do not use <c>2022-latest</c> —
/// floating CUs have crashed on GitHub Actions (ClockData / QpcFrequency assert).
/// </summary>
public static class MsSqlTestImage
{
    /// <summary>SQL Server 2022 CU20 on Ubuntu 22.04. Used by every Testcontainers fixture.</summary>
    public const string Server2022 = "mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04";
}
