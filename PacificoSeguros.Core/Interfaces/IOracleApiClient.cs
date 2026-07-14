using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IOracleApiClient
    {
        Task<(ApiOutcome Outcome, OracleInteraccionResponse? Response)> IniciarGestionAsync(OracleIniLlamadaRequest request);
        Task<(ApiOutcome Outcome, string? Body)> FinalizarGestionAsync(OracleFinLlamadaRequest request, long IdOracle);
        string BuildFinLlamadaUrl(long id);
    }
}
