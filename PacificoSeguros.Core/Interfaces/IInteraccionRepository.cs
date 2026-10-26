using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    public interface IInteraccionRepository
    {
        Task<IReadOnlyList<CtiInteraccion>> PopulateIniLLamada(int top, string origen, CancellationToken ct = default);

        // UpdateIniLLamada, ReleaseIniLLamadaClaim y MarkIniLLamadaConfirmedUnpersisted no
        // reciben CancellationToken a propósito: persisten (o liberan) un resultado que
        // Oracle ya resolvió, y un apagado del servicio no debe abortarlos a mitad de
        // camino — cortarlos ahí es exactamente lo que deja filas en Envio=2 sin rastro.
        Task<bool> UpdateIniLLamada(string jsonIni, string jsonRespuestaIni, int envioIniLLamada, string lastInteractionId, long idOracle, string urlOracle, string? resultado, string? motivo);
        Task<bool> ReleaseIniLLamadaClaim(string lastInteractionId);
        Task<bool> MarkIniLLamadaConfirmedUnpersisted(string lastInteractionId);

        Task<int> ReclaimOrphanedIniLLamada(int timeoutMinutes, CancellationToken ct = default);
        Task InsertMachineOracle(CancellationToken ct = default);
    }
}
