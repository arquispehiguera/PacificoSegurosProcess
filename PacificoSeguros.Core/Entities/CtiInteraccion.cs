namespace PacificoSeguros.Core.Entities
{
    public class CtiInteraccion
    {
        public string LastInteractionId { get; set; } = string.Empty;
        public string? Celular { get; set; }
        public string Proveedor { get; set; } = "COVISIAN";
        public DateTime? FechaIniLLamada { get; set; }
        public string Tipo { get; set; } = "EC";
        public string? AgenteId { get; set; }
        public string? Origen { get; set; }
        public DateTime? FechaFinLLamada { get; set; }
        public string? Resultado { get; set; }
        public string? Motivo { get; set; }
        public long IdOportunidad { get; set; } = 0;
        
    }
}
