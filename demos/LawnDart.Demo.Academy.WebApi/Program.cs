using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi;
using Scalar.AspNetCore;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using LawnDart;
using LawnDart.AspNetCore;
using LawnDart.Authorization;
using LawnDart.Authorization.AspNetCore;
using LawnDart.Demo.Academy.WebApi.Authorization;
using LawnDart.Demo.Academy.WebApi.Commands;
using LawnDart.Demo.Academy.WebApi.Domain.Events;
using LawnDart.Demo.Academy.WebApi.Handlers;
using LawnDart.Demo.Academy.WebApi.Projections;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Messaging.InMemory;
using LawnDart.Messaging.Outbox;
using LawnDart.Metadata;
using LawnDart.Outbox;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Security;
using System.Text;

// DEMO ONLY: the issuer, audience, and fallback key exist so the demo runs with no setup.
// Do not copy them into a real app.
const string DemoIssuer = "lawndart-academy-demo";
const string DemoAudience = "lawndart-academy-api";
const string DemoFallbackKey = "Academy-Demo-Secret-Key-32-Chars!";

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLawnDart(opts =>
{
    opts.RequireTenantId = false;
    opts.EnableAuthorization = false;
});

var ctx = builder.Services.AddBoundedContext("default")
    .WithEventTypes<StudentRegistered>()
    .WithCommandHandlers<RegisterStudentCommandHandler>();

var useSql = args.Contains("--sql")
    || builder.Configuration.GetValue("EventStore:UseSqlServer", false);

if (useSql)
{
    var connectionString =
        builder.Configuration.GetConnectionString("Academy")
        ?? Environment.GetEnvironmentVariable("LAWNDART_SQL_CONNECTION")
        ?? throw new InvalidOperationException(
            "SQL mode requires ConnectionStrings:Academy or LAWNDART_SQL_CONNECTION.");

    builder.Services.AddSqlProjectionStores("default", connectionString);
    builder.Services.AddInMemoryMessaging();
    builder.Services.AddMessageTransportOutboxPublisher();
    ctx.UseSqlServer(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.RequireTenantId = false;
            opts.EnableOutbox = true;
        })
        .WithProjections(
            [typeof(StudentSummaryProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 100;
            });
}
else
{
    builder.Services.AddInMemoryProjectionStores("default");
    ctx.UseInMemory()
        .WithProjections(
            [typeof(StudentSummaryProjection).Assembly],
            opts =>
            {
                opts.PollInterval = TimeSpan.FromMilliseconds(200);
                opts.CheckpointInterval = 100;
            });
}

builder.Services.AddHttpContextAccessor();
builder.Services.AddScoped<ITenantContextProvider, JwtTenantContextProvider>();

builder.Services.AddSingleton<IAuthorizationProvider, AcademyAuthorizationProvider>();
builder.Services.AddScoped<AuthorizationService>();
builder.Services.AddScoped<IAuthorizationContextProvider, HttpAuthorizationContextProvider>();

builder.Services.AddLawnDartHttpCommands(typeof(RegisterStudentCommand).Assembly);

var configuredKey = builder.Configuration["Jwt:Key"];
string jwtKey;
if (string.IsNullOrWhiteSpace(configuredKey))
{
    if (!builder.Environment.IsDevelopment())
    {
        throw new InvalidOperationException(
            "Jwt:Key is not configured. The built-in demo key is only allowed when ASPNETCORE_ENVIRONMENT=Development. Set Jwt:Key (for example with the Jwt__Key environment variable).");
    }

    // DEMO ONLY: fallback so the demo runs with no setup. Do not copy it into a real app.
    jwtKey = DemoFallbackKey;
}
else
{
    jwtKey = configuredKey;
    if (Encoding.UTF8.GetByteCount(jwtKey) < 32)
    {
        throw new InvalidOperationException(
            "Jwt:Key must be at least 32 bytes. HS256 needs a 256-bit key.");
    }
}

builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(opts =>
    {
        opts.MapInboundClaims = true;
        opts.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuer = DemoIssuer,
            ValidateAudience = true,
            ValidAudience = DemoAudience,
            ValidateLifetime = true,
            RequireExpirationTime = true,
            ValidateIssuerSigningKey = true,
            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey)),
            ClockSkew = TimeSpan.Zero
        };
    });

builder.Services.AddAuthorization();

builder.Services.AddOpenApi(opts =>
{
    opts.AddDocumentTransformer((doc, _, _) =>
    {
        doc.Components ??= new OpenApiComponents();
        doc.Components.SecuritySchemes ??= new Dictionary<string, IOpenApiSecurityScheme>();
        doc.Components.SecuritySchemes["BearerAuth"] = new OpenApiSecurityScheme
        {
            Type = SecuritySchemeType.Http,
            Scheme = "bearer",
            BearerFormat = "JWT",
            Description = "POST /token for a demo JWT, then paste it here (no 'Bearer ' prefix). " +
                          "Roles: Admin, Instructor, Student."
        };
        doc.Security =
        [
            new OpenApiSecurityRequirement
            {
                [new OpenApiSecuritySchemeReference("BearerAuth")] = []
            }
        ];
        return Task.CompletedTask;
    });
});

var app = builder.Build();

if (useSql)
{
    var store = app.Services.GetRequiredKeyedService<IEventStore>("default");
    if (store is not SqlServerEventStore sql)
    {
        throw new InvalidOperationException(
            "SQL mode did not resolve SqlServerEventStore for context 'default'.");
    }

    await sql.InitializeSchemaAsync().ConfigureAwait(false);
    await app.Services.GetRequiredKeyedService<IOutboxWriter>("default")
        .InitializeSchemaAsync()
        .ConfigureAwait(false);
    await app.Services.InitializeSqlProjectionStoresAsync("default").ConfigureAwait(false);
}

app.UseAuthentication();
app.UseAuthorization();

app.MapOpenApi();

app.MapScalarApiReference(opts =>
{
    opts.Title = "LawnDart Academy API";
    opts.Theme = ScalarTheme.Moon;
    opts.DefaultHttpClient = new(ScalarTarget.CSharp, ScalarClient.HttpClient);
});

app.MapLawnDartCommands();
app.MapProjectionQueries("default");

app.MapGet("/", () => Results.Redirect("/scalar/v1"))
    .ExcludeFromDescription();

app.MapGet("/health", () => Results.Ok(new { status = "healthy", time = DateTime.UtcNow }))
    .WithName("HealthCheck")
    .WithTags("System");

app.MapPost("/token", async (HttpRequest request) =>
    {
        var req = new TokenRequest(null, null, null);
        if (request.ContentLength is > 0)
        {
            var parsed = await request.ReadFromJsonAsync<TokenRequest>().ConfigureAwait(false);
            if (parsed is not null)
                req = parsed;
        }

        var role = string.IsNullOrWhiteSpace(req.Role) ? "Student" : req.Role.Trim();
        if (role is not ("Admin" or "Instructor" or "Student"))
            return Results.BadRequest(new { error = "role must be Admin, Instructor, or Student." });

        var tenantId = string.IsNullOrWhiteSpace(req.TenantId) ? "academy" : req.TenantId.Trim();
        var sub = string.IsNullOrWhiteSpace(req.Sub) ? "demo" : req.Sub.Trim();
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey));
        var token = new JwtSecurityTokenHandler().WriteToken(new JwtSecurityToken(
            issuer: DemoIssuer,
            audience: DemoAudience,
            claims:
            [
                new Claim("role", role),
                new Claim("tenant_id", tenantId),
                new Claim("sub", sub)
            ],
            notBefore: DateTime.UtcNow,
            expires: DateTime.UtcNow.AddHours(8),
            signingCredentials: new SigningCredentials(key, SecurityAlgorithms.HmacSha256)));

        return Results.Ok(new { token, role });
    })
    .AllowAnonymous()
    .WithName("IssueDemoToken")
    .WithTags("System");

app.Run();

internal sealed record TokenRequest(string? Role, string? TenantId, string? Sub);
