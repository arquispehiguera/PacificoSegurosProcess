using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IOracleApiClient
    {
        Task<OracleInteraccionResponse?> IniciarGestionAsync(OracleIniLlamadaRequest request);
        Task<string?> FinalizarGestionAsync(string urlOracle, OracleFinLlamadaRequest request);
        string BuildFinLlamadaUrl(long id);
    }
}
