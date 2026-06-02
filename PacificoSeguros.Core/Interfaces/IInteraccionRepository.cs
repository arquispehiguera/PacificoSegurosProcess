using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IInteraccionRepository
    {
        Task<IReadOnlyList<CtiInteraccion>> PopulateIniLLamada();
        Task<IReadOnlyList<CtiInteraccion>> PopulateFinLLamada();
        Task<bool> UpdateIniLLamada(string jsonIni, string jsonRespuestaIni, int envioIniLLamada, string lastInteractionId, long idOracle, string urlOracle);
        Task<bool> UpdateFinLLamada(string jsonFin, string jsonRespuestaFin, int envioFinLLamada, string lastInteractionId);
    }
}
