using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IInteraccionRepository
    {
        Task<IReadOnlyList<CtiInteraccion>> PopulateIniLLamada(int top);
        Task<IReadOnlyList<CtiInteraccion>> PopulateFinLLamada(int top);
        Task<bool> UpdateIniLLamada(string jsonIni, string jsonRespuestaIni, int envioIniLLamada, string lastInteractionId, long idOracle, string urlOracle);
        Task<bool> UpdateFinLLamada(string jsonFin, string jsonRespuestaFin, int envioFinLLamada, string lastInteractionId);
        Task<bool> ReleaseIniLLamadaClaim(string lastInteractionId);
        Task<bool> ReleaseFinLLamadaClaim(string lastInteractionId);
        Task InsertMachineOracle();
    }
}
