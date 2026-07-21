namespace PacificoSeguros.Core.Entities
{
    public class OracleIniLlamadaRequest
    {
        public string? tANI_c { get; set; }
        public string? tProveedor_c { get; set; }
        public string? dInicio_c { get; set; }
        public string? tUCID_c { get; set; }
        public string? chTipo_c { get; set; }
        public long chOpty_Id_c { get; set; }
        public string? tUsuarioNumDoc_c { get; set; }

        // MACHINE no pasa por FinLlamada: viaja con valor real acá. AGENT todavía no
        // conoce el resultado en este punto (recién lo sabe al cerrar la llamada, en
        // FinLlamada) — el serializer se configura con NullValueHandling.Ignore
        // (OracleApiClient) para que la propiedad no aparezca en el JSON cuando es null.
        public string? chOptyTipifResultado_c { get; set; }
        public string? chOptyTipifSubResultado_c { get; set; }
    }
}
