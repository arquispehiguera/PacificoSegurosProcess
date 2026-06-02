namespace PacificoSeguros.Core.Entities
{
    public class CtiInteraccion
    {
        public string LastInteractionId { get; set; } = string.Empty;
        public string? ContactId { get; set; }
        public string? Celular { get; set; }
        public string Proveedor { get; set; } = "COVISIAN";
        public DateTime? FechaIniLLamada { get; set; }
        public string Tipo { get; set; } = "EC";
        public string? AgenteId { get; set; }
        public string? JsonIni { get; set; }
        public string? RespuestaIni { get; set; }
        public long? IdOracle { get; set; }
        public string? UrlOracle { get; set; }
        public int EnvioIniLLamada { get; set; } = 0;
        public DateTime? FechaFinLLamada { get; set; }
        public string? JsonFin { get; set; }
        public string? RespuestaFin { get; set; }
        public int EnvioFinLLamada { get; set; } = 0;
        public DateTime FechaRegistro { get; set; } = DateTime.Now;
    }
}
