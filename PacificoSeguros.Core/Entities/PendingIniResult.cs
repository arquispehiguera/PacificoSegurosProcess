namespace PacificoSeguros.Core.Entities
{
    // Resultado no transitorio de Oracle (Success o PermanentFailure) guardado en el
    // outbox local antes de intentar persistirlo en SQL. Lleva exactamente los mismos
    // datos que recibe UpdateIniLLamada, para que el replay pueda reaplicar ese mismo
    // UPDATE (idempotente) sin volver a consultar a Oracle — Oracle no ofrece un GET
    // por tUCID_c, así que este archivo es la única prueba local de que ya se envió.
    public class PendingIniResult
    {
        public string JsonIni { get; set; } = string.Empty;
        public string RespuestaIni { get; set; } = string.Empty;
        public int EnvioIniLLamada { get; set; }
        public string LastInteractionId { get; set; } = string.Empty;
        public long IdOracle { get; set; }
        public string UrlOracle { get; set; } = string.Empty;
        public string? Resultado { get; set; }
        public string? Motivo { get; set; }
        // Antigüedad del resultado pendiente: el replay la usa para decidir cuándo un
        // archivo que no logra reaplicarse pasa a expired/. Si llega en default (archivo
        // escrito por una versión anterior), el outbox la completa con la fecha del archivo.
        public DateTime CreatedAtUtc { get; set; }
    }

    // Qué hizo el mantenimiento del outbox con un archivo puntual. Viaja como dato y no
    // como log porque el que loguea es el servicio: ahí se empuja el ContactId al
    // LogContext para que cada línea quede correlacionada con su LastInteractionId.
    public enum OutboxMaintenanceAction
    {
        // .json.tmp abandonado: un SaveAsync que nunca llegó al Move.
        TempDeleted,
        // .json ilegible movido a quarantine/ para revisión manual.
        Quarantined,
        // Archivo de expired/ o quarantine/ que superó la retención.
        ArchiveDeleted,
        // Error de IO al procesar el archivo; se deja como está y se reintenta el próximo ciclo.
        Failed
    }

    public class OutboxMaintenanceEntry
    {
        // Para archivos ilegibles no hay contenido confiable: se usa el nombre del
        // archivo sin extensión, que es el LastInteractionId saneado.
        public string LastInteractionId { get; set; } = string.Empty;
        public string FileName { get; set; } = string.Empty;
        public OutboxMaintenanceAction Action { get; set; }
        // Ruta destino (cuarentena) o descripción de la operación que falló.
        public string? Detail { get; set; }
        public Exception? Error { get; set; }
    }

    public class OutboxMaintenanceResult
    {
        public List<OutboxMaintenanceEntry> Entries { get; } = new();
    }
}
