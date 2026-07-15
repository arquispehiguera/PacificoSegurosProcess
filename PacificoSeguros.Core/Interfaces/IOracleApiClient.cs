using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IOracleApiClient
    {
        Task<(ApiOutcome Outcome, OracleInteraccionResponse? Response)> IniciarGestionAsync(OracleIniLlamadaRequest request, CancellationToken ct);
        Task<(ApiOutcome Outcome, string? Body)> FinalizarGestionAsync(OracleFinLlamadaRequest request, long IdOracle, CancellationToken ct);
        string BuildFinLlamadaUrl(long id);
    }
}
