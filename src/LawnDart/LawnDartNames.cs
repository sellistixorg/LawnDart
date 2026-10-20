using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace LawnDart;

/// <summary>
/// Format rule for bounded context names and tenant ids.
/// </summary>
/// <remarks>
/// The spelling you pass is the spelling that is stored. A second context
/// whose name differs only by case is rejected. The default context is
/// <see cref="DefaultContextName"/>. Keyed lookup of the unkeyed alias uses
/// that exact spelling.
/// </remarks>
public static class LawnDartNames
{
    /// <summary>
    /// A letter or digit, then up to 62 letters, digits, dots, underscores, or dashes.
    /// 63 characters at most.
    /// </summary>
    public const string Pattern = "^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$";

    /// <summary>Maximum length of a context name or tenant id.</summary>
    public const int MaxLength = 63;

    /// <summary>
    /// Reserved spelling of the default bounded context.
    /// The unkeyed service alias is registered only for this exact spelling.
    /// </summary>
    public const string DefaultContextName = "default";

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="value"/> matches <see cref="Pattern"/>.
    /// Null and empty return <see langword="false"/>.
    /// </summary>
    public static bool IsValid(string? value)
    {
        if (value is null || value.Length == 0 || value.Length > MaxLength)
            return false;

        if (!IsAsciiLetterOrDigit(value[0]))
            return false;

        for (var i = 1; i < value.Length; i++)
        {
            var c = value[i];
            if (!IsAsciiLetterOrDigit(c) && c != '.' && c != '_' && c != '-')
                return false;
        }

        return true;
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> when <paramref name="value"/> does not match <see cref="Pattern"/>.
    /// </summary>
    /// <param name="value">Context name or tenant id.</param>
    /// <param name="paramName">Parameter name for the exception.</param>
    public static void Validate(string? value, string paramName)
    {
        ArgumentNullException.ThrowIfNull(paramName);
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Name must not be null or whitespace.", paramName);

        if (!IsValid(value))
            throw new ArgumentException(FormatMessage(value), paramName);
    }

    /// <summary>
    /// Checks a bounded context name.
    /// Null or whitespace is rejected.
    /// A spelling of <see cref="DefaultContextName"/> that differs only by case is rejected.
    /// When <paramref name="validateFormat"/> is <see langword="true"/>, the name must match <see cref="Pattern"/>.
    /// </summary>
    /// <param name="contextName">Bounded context name.</param>
    /// <param name="paramName">Parameter name for the exception.</param>
    /// <param name="validateFormat">
    /// When <see langword="false"/>, the character rule is skipped.
    /// The reserved <see cref="DefaultContextName"/> spelling is still enforced.
    /// </param>
    public static void ValidateContextName(string? contextName, string paramName, bool validateFormat = true)
    {
        ArgumentNullException.ThrowIfNull(paramName);
        if (string.IsNullOrWhiteSpace(contextName))
            throw new ArgumentException("Context name must not be null or whitespace.", paramName);

        if (IsWrongDefaultSpelling(contextName))
        {
            throw new ArgumentException(
                $"Context name '{contextName}' is reserved. Register the default context as '{DefaultContextName}'. " +
                "Keyed lookup of the unkeyed alias uses that exact spelling.",
                paramName);
        }

        if (validateFormat && !IsValid(contextName))
            throw new ArgumentException(FormatMessage(contextName), paramName);
    }

    /// <summary>
    /// Checks a tenant id.
    /// Null, empty, or whitespace is rejected.
    /// A colon is always rejected, because a stream id is split on every colon.
    /// When <paramref name="validateFormat"/> is <see langword="true"/>, the spelling
    /// <c>default</c> (<see cref="DefaultContextName"/>) is reserved, and the id must match <see cref="Pattern"/>.
    /// </summary>
    /// <param name="tenantId">Tenant id.</param>
    /// <param name="paramName">Parameter name for the exception.</param>
    /// <param name="validateFormat">
    /// When <see langword="false"/>, the character rule is skipped and a tenant id of
    /// <see cref="DefaultContextName"/> is allowed.
    /// An empty string is still rejected. A colon is still rejected.
    /// </param>
    public static void ValidateTenantId(string? tenantId, string paramName, bool validateFormat = true)
    {
        ArgumentNullException.ThrowIfNull(paramName);
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        if (tenantId.Contains(':', StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tenant id '{tenantId}' must not contain ':'. A stream id is '{{tenantId}}:{{type}}:{{id}}' and is split on every colon.",
                paramName);
        }

        if (validateFormat && string.Equals(tenantId, DefaultContextName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tenant id '{DefaultContextName}' is reserved. Set ValidateNames to false to keep an existing tenant id of '{DefaultContextName}'.",
                paramName);
        }

        if (validateFormat && !IsValid(tenantId))
            throw new ArgumentException(FormatMessage(tenantId), paramName);
    }

    /// <summary>
    /// Reads <see cref="LawnDartOptions.ValidateNames"/> from options already configured
    /// on <paramref name="services"/>. Returns <see langword="true"/> when no options are configured.
    /// </summary>
    internal static bool IsFormatCheckEnabled(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var options = new LawnDartOptions();
        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(IConfigureOptions<LawnDartOptions>) &&
                descriptor.ImplementationInstance is IConfigureOptions<LawnDartOptions> configure)
            {
                configure.Configure(options);
            }
        }

        foreach (var descriptor in services)
        {
            if (descriptor.ServiceType == typeof(IPostConfigureOptions<LawnDartOptions>) &&
                descriptor.ImplementationInstance is IPostConfigureOptions<LawnDartOptions> post)
            {
                post.PostConfigure(Options.DefaultName, options);
            }
        }

        return options.ValidateNames;
    }

    /// <summary>
    /// Applies <see cref="ValidateContextName"/> using <see cref="LawnDartOptions.ValidateNames"/>
    /// already configured on <paramref name="services"/>.
    /// </summary>
    internal static void RequireContextName(IServiceCollection services, string? contextName, string paramName)
        => ValidateContextName(contextName, paramName, IsFormatCheckEnabled(services));

    /// <summary>
    /// Applies <see cref="ValidateContextName"/> using <see cref="LawnDartOptions.ValidateNames"/>
    /// from the built provider. Missing options keep the check on.
    /// </summary>
    internal static void RequireContextName(IServiceProvider services, string? contextName, string paramName)
    {
        ArgumentNullException.ThrowIfNull(services);
        var options = services.GetService<IOptions<LawnDartOptions>>();
        var enabled = options?.Value.ValidateNames ?? true;
        ValidateContextName(contextName, paramName, enabled);
    }

    private static bool IsWrongDefaultSpelling(string contextName) =>
        contextName.Equals(DefaultContextName, StringComparison.OrdinalIgnoreCase)
        && !contextName.Equals(DefaultContextName, StringComparison.Ordinal);

    private static string FormatMessage(string value) =>
        $"'{value}' does not match {Pattern}. A name starts with a letter or digit, " +
        $"then letters, digits, '.', '_', or '-', and is at most {MaxLength} characters.";

    private static bool IsAsciiLetterOrDigit(char c) =>
        c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9';
}
