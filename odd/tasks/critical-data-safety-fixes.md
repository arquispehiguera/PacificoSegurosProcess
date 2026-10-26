# Feature: critical-data-safety-fixes

## Objective
Apply only the minimal changes that prevent data damage: duplicate gestiones in Oracle and rows wrongly lost as Envio=3. No schema changes, no new states.

## Why
Architecture review (2026-10-23) found: unbounded claiming lets rows wait in the channel longer than ClaimTimeoutMinutes → reclaim → same row sent twice (C1); non-idempotent POST retried on 500/502/504/timeout (C2); shutdown cancellation recorded as PermanentFailure (H1); token failures recorded as PermanentFailure per row (H2). User decision: keep it simple, critical only, no ReservaId column, no Envio=5 (unknown outcomes go to Envio=3 with HTTP status + body stored).

## Tasks
- [x] T1 — Claim only free channel capacity (route: delegated, writer trigger: 3 non-trivial files)
- [x] T2 — No automatic retry on 500/502/504/timeout; Envio=3 with status + body in RespuestaIni (same writer)
- [x] T3 — Shutdown cancellation propagates, nothing persisted (same writer)
- [x] T4 — Token failures → TransientFailure (same writer)

## Acceptance criteria
- `dotnet build PacificoSeguros.sln` 0 errors, 0 warnings.
- 429/503/connection failures still retried; 500/502/504/HttpClient timeout never retried and never released to Envio=0.
- OperationCanceledException from the stopping token never results in Envio=3.

## Constraints
No git repo (no commits/review). No test project; build-only verification. Never run the service.

## Progress
- Created 2026-10-23.
- T1-T4 done by delegated writer; build 0 warnings / 0 errors. Parent spot check: build 0 errors; IsRetryableForIniLlamada = 429/503 only (OracleApiClient.cs:319), token keeps 5xx.
- Deviation: unreadable token JSON / empty access_token also → TransientFailure.
- Residual risks: HttpRequestException while reading a response body is still retried (rare duplicate); 503 assumed not processed; per-pool backlog = BatchSize + workers.
- Skipped: commits/review (no git), runtime tests (no test project).

## Next step
Deploy. Envio=3 rows with RespuestaIni starting "[500]", "[502]", "[504]" or "[timeout]" need manual check in Oracle (gestión may exist).
