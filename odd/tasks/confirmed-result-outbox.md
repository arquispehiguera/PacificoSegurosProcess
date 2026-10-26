# Feature: confirmed-result-outbox

## Objective
Make persistence of Oracle results resilient to SQL Server outages so a result Oracle already confirmed is never lost nor re-sent (duplicate gestión) by the orphan-reclaim timeout.

## Problem / Why
Nightly (~22:00) and on 2026-10-22 09:16 the service lost network reach to SQL (`error 40` / Win32 `53`). If Oracle succeeds and both `UpdateIniLLamada` and `MarkIniLLamadaConfirmedUnpersisted` fail during an outage, the row stays `Envio=2` and `ReclaimOrphanedIniLLamada` re-sends it after 20 min, creating a duplicate in Oracle. Oracle offers no GET to check by `tUCID_c`, so idempotency must be local.

## Scope
- Local file outbox: one atomic JSON file per `LastInteractionId`, written right after a non-transient Oracle outcome and before the SQL update; removed after the SQL update succeeds.
- Producer replays pending outbox files before the orphan reclaim each cycle; a failed replay skips reclaim for that cycle.
- DB connection hardening: `OpenAsync`, optional `CancellationToken` on claim/reclaim/insert paths (not on confirmed-result persistence), `tcp:` prefix in connection strings.

## Constraints
- No behavior change for the happy path; existing Envio states (0/1/2/3/4) keep their meaning.
- Interface changes must be source-compatible (optional parameters).
- No git repository present (removed by user before migration): work-unit commits and native review are not possible; recorded as skipped.
- No test project exists; verification is `dotnet build` plus structural review. Running the service locally is not allowed (it targets production SQL and Oracle).

## Tasks
- [x] T1 — Outbox abstraction + file implementation + DI/config (route: delegated, writer trigger: 2+ non-trivial files)
- [x] T2 — Write/remove outbox in `ProcessIniLlamada`; replay in Producer and debug loop before reclaim (route: delegated, same writer)
- [x] T3 — DB hardening: `OpenAsync`, optional `CancellationToken`, `tcp:` prefix (route: delegated, same writer)
- [x] T4 — Outbox maintenance (route: delegated, writer trigger: 3+ non-trivial files). User-approved 2026-10-23:
  - `.json.tmp` older than 10 min → delete.
  - Corrupt `.json` → move to `outbox/quarantine/`.
  - `.json` older than 1 h whose replay UPDATE fails → `MarkIniLLamadaConfirmedUnpersisted` (Envio 2→4) succeeds or returns false → move to `outbox/expired/`; if Mark throws (DB down) → keep file and stop replay (no reclaim). Rationale: moving a file while the row is still Envio=2 would let reclaim re-send it (duplicate).
  - `expired/` and `quarantine/` files older than 7 days → delete.
  - All thresholds configurable under `Outbox`.
  - Every outbox Error/Warning log carries `LastInteractionId` in the template and in the `ContactId` LogContext property (that column stores LastInteractionId by design; user rule: LastInteractionId is unique per call, ContactId is the contact).

## Acceptance criteria
- Solution builds with 0 errors and no new warnings.
- Outbox file is written atomically (temp file + flush + move) under a path resolved against `AppContext.BaseDirectory`.
- Replay runs before `ReclaimOrphanedIniLLamada`; if replay throws, reclaim does not run that cycle.
- Confirmed-result persistence (`UpdateIniLLamada`, `MarkIniLLamadaConfirmedUnpersisted`) is not cancelled by the shutdown token.

## Checks
- `dotnet build PacificoSeguros.sln`

## Progress / Evidence
- Baseline build: OK, 0 warnings, 0 errors (2026-10-23).
- T1–T3 done by delegated writer; writer build `--no-incremental`: 0 warnings, 0 errors.
- Parent spot check: `dotnet build PacificoSeguros.sln` → 0 errors; replay precedes reclaim (CtiInteraccionBackgroundService.cs:178 → :184); both connection strings now `Data Source=tcp:<host>,1433`.
- Skipped: work-unit commits and native review (no git repo); runtime test (no test project, service targets production).
- Known residual risks: corrupt/.tmp outbox files are not auto-cleaned; a row whose replay UPDATE keeps failing blocks reclaim every cycle (safe, needs monitoring); Program.cs forces cwd to `C:\JobsDeployment\PacificoSegurosProcess` while outbox resolves against `AppContext.BaseDirectory` — set an absolute `Outbox:Path` in production.

- T4 done by delegated writer (build 0/0, also `--no-incremental`); parent spot check build 0 errors, `Outbox` config has the 4 keys.
- T4 deviations: stale item with connectivity error is never expired (stricter); `MoveToExpiredAsync` returns destination path; result types in `PendingIniResult.cs`; connectivity classifier lives in Process.
- T4 residual risk: if SQL drops between the failed replay UPDATE (server-side error) and Mark, Mark returns false (it swallows exceptions) and the file may move to expired while row is Envio=2 → possible reclaim re-send. Narrow window; documented in code. Full fix requires Mark to rethrow instead of returning false (repository change, not authorized in T4).
- Deployed path confirmed by user: `C:\JobsDeployment\PacificoSegurosProcess` (relative `outbox` resolves there).

## Next step
Deploy updated binaries + appsettings.json; watch logs at the next ~22:00 outage for "outbox" replay messages.
