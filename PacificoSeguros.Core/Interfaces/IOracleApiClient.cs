using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IOracleApiClient
    {
        Task<OracleInteraccionResponse?> IniciarGestionAsync(OracleIniLlamadaRequest request);
        Task<string?> FinalizarGestionAsync( OracleFinLlamadaRequest request,long IdOracle);
        string BuildFinLlamadaUrl(long id);
    }
}
