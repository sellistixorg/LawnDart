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

`GET /health` is unauthenticated.

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

Nothing in this demo **issues** a JWT. There is no login or `/token` endpoint.
The host only **validates** `Authorization: Bearer <jwt>` with HS256.

Signing key (`Jwt:Key` in `appsettings.json`):

```text
Academy-Demo-Secret-Key-32-Chars!
```

Issuer, audience, and expiry are not validated. You mint the token yourself
and paste it into Scalar (Authorize → BearerAuth, token only, no `Bearer `
prefix).

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

### Create a JWT (jwt.io)

1. Algorithm **HS256**.
2. Payload:

```json
{
  "role": "Admin",
  "tenant_id": "academy",
  "sub": "demo"
}
```

3. Secret: `Academy-Demo-Secret-Key-32-Chars!`
4. Leave **secret base64 encoded** unchecked (the key is a raw UTF-8 string).
5. Copy the encoded token into Scalar.

Use the same `tenant_id` on later GETs as on the POSTs that wrote the streams
(`academy` in the example above).

### Create a JWT (PowerShell)

```powershell
function B64Url([byte[]]$bytes) {
    [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+','-').Replace('/','_')
}
$header  = B64Url ([Text.Encoding]::UTF8.GetBytes('{"alg":"HS256","typ":"JWT"}'))
$payload = B64Url ([Text.Encoding]::UTF8.GetBytes('{"role":"Admin","tenant_id":"academy","sub":"demo"}'))
$unsigned = "$header.$payload"
$key = [Text.Encoding]::UTF8.GetBytes('Academy-Demo-Secret-Key-32-Chars!')
$hmac = [System.Security.Cryptography.HMACSHA256]::new($key)
$jwt = "$unsigned.$(B64Url $hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($unsigned)))"
$jwt
```

Then:

```http
Authorization: Bearer <jwt>
```
