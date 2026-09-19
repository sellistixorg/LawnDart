# LawnDart Academy Web API

HTTP host for the Academy domain: command POSTs, Lightweight projection GETs,
JWT permission checks. There is **no app UI**. Scalar at `/scalar/v1` is the
only page (the root redirects there).

Requires the .NET 10 SDK. Default store is InMemory (no Docker).

This project does **not** reference the console demo. Commands, handlers, and
projections live here.

## Run

```bash
dotnet run --project demos/LawnDart.Demo.Academy.WebApi
```

Listens on [http://localhost:5180](http://localhost:5180). Open
[http://localhost:5180/scalar/v1](http://localhost:5180/scalar/v1).

Optional SQL Server (running instance + connection string):

```bash
dotnet run --project demos/LawnDart.Demo.Academy.WebApi --launch-profile SqlServer
```

Or `dotnet run -- --sql` with `ConnectionStrings:Academy` or
`LAWNDART_SQL_CONNECTION`.

`GET /health` and `POST /token` are unauthenticated.

## Endpoints

`MapLawnDartCommands` derives POST routes from command type names:

| POST | Permission |
|---|---|
| `/api/web-api/register-student` | `Student.Enroll` |
| `/api/web-api/enroll-student` | `Student.Enroll` |
| `/api/web-api/cancel-enrollment` | `Student.Enroll` |
| `/api/web-api/create-course-section` | `Section.Create` |

Projection GETs:

| GET | Permission |
|---|---|
| `/api/views/students/{studentId}` | `Student.View` |
| `/api/views/sections/{sectionId}` | `Section.View` |
| `/api/views/enrollment-index` | `EnrollmentIndex.View` |

A command without a bearer token returns **403**. A view GET without a token
returns **401**. After a successful POST, wait a moment for the projection
runner before the matching GET.

Example command body:

```json
{
  "id": "11111111-1111-1111-1111-111111111111",
  "studentId": "22222222-2222-2222-2222-222222222222",
  "name": "Ada Lovelace",
  "email": "ada@academy.test"
}
```

`id` is the command id (new GUID per request). `studentId` / `sectionId` are
the stream identities.

## Authentication

Call `POST /token` for a demo JWT. Paste `token` into Scalar (Authorize,
BearerAuth, no `Bearer ` prefix). Then POST commands and GET views.

This is not login. The host signs with HS256 using `Jwt:Key` in
`appsettings.json` and does not check issuer, audience, or expiry. Use the
same key if you mint a token offline.

```bash
curl -s -X POST http://localhost:5180/token -H "Content-Type: application/json" -d "{\"role\":\"Admin\"}"
```

An empty body issues `Student` / `academy` / `demo`. Allowed roles:
`Admin`, `Instructor`, `Student`. Any other role is `400`.

Use the same `tenant_id` on later GETs as on the POSTs that wrote the streams
(`academy` unless you passed `tenantId`).

### Claims the host reads

| Claim | Purpose |
|---|---|
| `role` | Mapped to ASP.NET `ClaimTypes.Role`. `Admin` passes every permission check. |
| `tenant_id` (or `tid`) | Tenant for stream IDs and tenant-scoped views. Handlers fall back to `default` if it is missing. |
| `sub` | Optional user id. |

### Roles

| Role | Permissions |
|---|---|
| `Admin` | All |
| `Instructor` | `Section.Create`, `Section.View` |
| `Student` | `Student.View`, `Student.Enroll`, `Section.View`, `EnrollmentIndex.View` |

`EnrollmentIndex` is system-global (no tenant in the view key) but still
requires `EnrollmentIndex.View`.
