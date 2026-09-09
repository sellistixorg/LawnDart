## Summary

-

## Validation

- [ ] `dotnet build --configuration Release`
- [ ] `dotnet test --filter "Category!=Integration" --configuration Release` (no Docker)
- [ ] CI also runs `Category=Integration` (SQL Server / Testcontainers)
- [ ] If docs changed: `dotnet docfx metadata docfx.json` (when API DLLs exist)
- [ ] If docs changed: `dotnet docfx build docfx.json`
- [ ] Public and protected members have XML `<summary>` docs
- [ ] `CHANGELOG.md` updated under `## [Unreleased]`
- [ ] No new third-party runtime dependency (or a linked issue agreeing to one)
- [ ] All commits signed off — see
      [Developer Certificate of Origin](../CONTRIBUTING.md#developer-certificate-of-origin)

## Notes

-
