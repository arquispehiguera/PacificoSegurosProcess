using PacificoSeguros.Core.Entities;

namespace PacificoSeguros.Core.Interfaces
{
    // Almacenamiento local y durable de resultados que Oracle ya resolvió pero que
    // todavía no se confirmaron en SQL. Existe para que una caída de SQL Server no
    // deje la fila en Envio=2 sin rastro: si eso pasa, el reclamo por timeout la
    // devolvería a 0 y se reenviaría, creando una gestión duplicada en Oracle.
    public interface IConfirmedResultOutbox
    {
        // Debe quedar escrito de forma atómica y durable antes de retornar.
        Task SaveAsync(PendingIniResult result);

        // Idempotente: no falla si el registro ya no existe.
        Task RemoveAsync(string lastInteractionId);

        // Ordenado por CreatedAtUtc (completado con la fecha del archivo si viene en
        // default). Los registros ilegibles se omiten acá; RunMaintenanceAsync es el
        // único lugar que los mueve a cuarentena.
        Task<IReadOnlyList<PendingIniResult>> ListPendingAsync();

        // Mueve el registro pendiente a expired/ (sin pisar uno previo) y devuelve la
        // ruta destino. Solo debe llamarse cuando la fila ya no puede ser reenviada por
        // el reclamo por timeout (Envio distinto de 2).
        Task<string> MoveToExpiredAsync(string lastInteractionId);

        // Limpieza solo de disco, sin tocar la base: borra .tmp abandonados, mueve los
        // .json ilegibles a quarantine/ y purga expired/ y quarantine/ por retención.
        // Nunca toca un .json válido de la raíz y no lanza por errores de IO de un
        // archivo puntual: los informa en el resultado y sigue con el resto.
        Task<OutboxMaintenanceResult> RunMaintenanceAsync();
    }
}
