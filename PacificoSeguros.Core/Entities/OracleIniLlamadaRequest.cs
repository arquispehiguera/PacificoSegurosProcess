namespace PacificoSeguros.Core.Entities
{
    public class OracleIniLlamadaRequest
    {
        public string? tANI_c { get; set; }
        public string? tProveedor_c { get; set; }
        public string? dInicio_c { get; set; }

        // Origen=MACHINE: mismo valor que dInicio_c (un solo evento, no hay agente que
        // lo cierre después). Origen=AGENT: dFin_c real, tomado de FechaFinLLamada —
        // este método ya no se dispara para AGENT hasta que ese dato existe (ver
        // PopulateIniLLamada en InteraccionRepository).
        public string? dFin_c { get; set; }
        public string? tUCID_c { get; set; }
        public string? chTipo_c { get; set; }
        public long chOpty_Id_c { get; set; }
        public string? tUsuarioNumDoc_c { get; set; }

        // MACHINE: la tipificación ya se conoce al armar este request, viaja con valor
        // real. AGENT: todavía no la tenemos en nuestra tabla en este punto — viaja en
        // null y se recupera de la respuesta de este mismo llamado (OracleInteraccionResponse).
        // El serializer se configura con NullValueHandling.Ignore (OracleApiClient)
        // para que la propiedad no aparezca en el JSON cuando es null.
        public string? chOptyTipifResultado_c { get; set; }
        public string? chOptyTipifSubResultado_c { get; set; }
    }
}
