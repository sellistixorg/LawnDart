using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi;
using Scalar.AspNetCore;
using LawnDart;
using LawnDart.AspNetCore;
using LawnDart.Authorization;
using LawnDart.Authorization.AspNetCore;
using LawnDart.Demo.Academy.WebApi.Authorization;
using LawnDart.Demo.Academy.WebApi.Commands;
using LawnDart.Demo.Academy.WebApi.Domain.Events;
using LawnDart.Demo.Academy.WebApi.Projections;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Security;
using System.Text;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLawnDart(opts =>
{
    opts.RequireTenantId = false;
    opts.EnableAuthorization = false;
});

builder.Services.AddInMemoryProjectionStores("default");
var ctx = builder.Services.AddBoundedContext("default")
    .WithEventTypes(typeof(StudentRegistered).Assembly);

var useSql = args.Contains("--sql")
    || builder.Configuration.GetValue("EventStore:UseSqlServer", false);

if (useSql)
{
    var connectionString =
        builder.Configuration.GetConnectionString("Academy")
        ?? Environment.GetEnvironmentVariable("LAWNDART_SQL_CONNECTION")
        ?? throw new InvalidOperationException(
            "SQL mode requires ConnectionStrings:Academy or LAWNDART_SQL_CONNECTION.");

    ctx.UseSqlServer(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.RequireTenantId = false;
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

var jwtKey = builder.Configuration["Jwt:Key"] ?? "Academy-Demo-Secret-Key-32-Chars!";
builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(opts =>
    {
        opts.MapInboundClaims = true;
        opts.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = false,
            ValidateAudience = false,
            ValidateLifetime = false,
            RequireExpirationTime = false,
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
            Description = "Paste your JWT token (without 'Bearer ' prefix). " +
                          "Use role 'Admin' for full access, 'Instructor' or 'Student' for scoped access."
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

app.Run();
