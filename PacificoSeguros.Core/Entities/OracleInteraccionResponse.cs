namespace PacificoSeguros.Core.Entities
{
    public class OracleInteraccionResponse
    {
        public long Id { get; set; }
        public string? tURL_c { get; set; }

        // Solo se usan para Origen=AGENT: Oracle ya trae la tipificación resuelta en
        // este mismo response porque el agente la cargó antes de que se dispare este
        // llamado (ver PopulateIniLLamada). Para MACHINE vienen vacíos y se ignoran —
        // ese caso ya manda su propia tipificación en el request.
        public string? chOptyTipifResultado_c { get; set; }
        public string? chOptyTipifSubResultado_c { get; set; }
    }
}
