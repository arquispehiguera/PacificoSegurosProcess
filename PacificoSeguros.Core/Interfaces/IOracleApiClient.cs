using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IOracleApiClient
    {
        Task<(ApiOutcome Outcome, OracleInteraccionResponse? Response, string? RawBody)> IniciarGestionAsync(OracleIniLlamadaRequest request, string origen, CancellationToken ct);
    }
}
