# Feature: dead-code-cleanup

## Objective
Remove verified unused code, unread SELECT columns, unused config keys and unused packages (section A of the 2026-10-23 dead-code analysis). No behavior change.

## Scope (user-approved: "aplica solo la A")
- Code: unused field `_configuration` in CtiInteraccionBackgroundService; discarded tuple item in the token call; constant `method` parameter of `BuildRequest`; redundant `using System.Net.Http;` in Program.cs; stale FinLlamada comments.
- CtiInteraccion properties never read + same columns removed from HydrateAsync SELECT: JsonIni, RespuestaIni, IdOracle, UrlOracle, EnvioIniLLamada, JsonFin, RespuestaFin, EnvioFinLLamada, FechaRegistro. DB columns untouched.
- Config: OracleApi:FinLlamadaPath, AllowedHosts, Logging section.
- Packages: 7 Serilog packages + Microsoft.Extensions.Configuration in Infraestructure (add explicit Microsoft.Extensions.Configuration.Binder); duplicate Microsoft.Extensions.Hosting.Abstractions in Process.

## Out of scope
Section B (Proveedor/Tipo defaults, sink config, Watchdog key drift) and section C (architecture).

## Tasks
- [x] T1 — Apply section A (route: delegated, writer trigger: 2+ non-trivial files)

## Checks
`dotnet build PacificoSeguros.sln` 0 errors / 0 warnings. No git, no tests; never run the service.

## Progress
- 2026-10-23: T1 done by delegated writer, nothing skipped. Parent check: `dotnet build --no-incremental` 0 errors; HydrateAsync SELECT reduced to 11 columns. Binder 9.0.9 added explicitly to Infraestructure.
