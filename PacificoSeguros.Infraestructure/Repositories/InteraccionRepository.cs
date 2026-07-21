using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;
using PacificoSeguros.Infraestructure.Data;
using System.Data;
using System.Globalization;

namespace PacificoSeguros.Infraestructure.Repositories
{
    public class InteraccionRepository : IInteraccionRepository
    {
        private readonly DbContextApp _context;
        private readonly ILogger<InteraccionRepository> _logger;
        private readonly IConfiguration _configuration;

        public InteraccionRepository(DbContextApp context, ILogger<InteraccionRepository> logger, IConfiguration configuration)
        {
            _context = context;
            _logger = logger;
            _configuration = configuration;
        }

        // OracleApi:ReprocessDate ("yyyy-MM-dd") pisa el filtro de "hoy" en los dos
        // Populate — pensado para reprocesar un día puntual a mano. Vacío/ausente (caso
        // normal) devuelve null y el SQL cae al mismo GETDATE()-5h de siempre vía COALESCE.
        private DateTime? GetReprocessDate()
        {
            var raw = _configuration["OracleApi:ReprocessDate"];
            if (string.IsNullOrWhiteSpace(raw))
                return null;

            if (!DateTime.TryParseExact(raw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                _logger.LogWarning("OracleApi:ReprocessDate ({Raw}) no tiene formato yyyy-MM-dd — se ignora, se usa el día de hoy", raw);
                return null;
            }

            return date;
        }

        public async Task<IReadOnlyList<CtiInteraccion>> PopulateIniLLamada(int top)
        {
            // GETDATE() en este SQL Server corre ~5 horas adelantado respecto a la hora
            // local del negocio (confirmado con SELECT GETDATE() en producción). Sin el
            // DATEADD(HOUR,-5,...), cualquier registro creado entre ~19hs y medianoche
            // hora local quedaría permanentemente fuera del filtro de "hoy" — el server
            // ya cree que es el día siguiente. Si algún día se corrige el reloj del
            // server, este ajuste hay que sacarlo.
            //
            // Claim atómico en dos pasos: el UPDATE marca la fila como reservada
            // (Envio = 2) y solo devuelve LastInteractionId — la clave del clustered
            // index, que el índice IX_GSS_OraclePacifico_EnvioIni_Fecha ya trae consigo
            // sin necesidad de key lookup. Devolver acá las 16 columnas (como se hacía
            // antes) obligaba a un key lookup al clustered index por cada fila, sosteniendo
            // el lock de escritura más tiempo del necesario — eso fue lo que generó timeouts
            // en producción apenas se desplegó. El resto de las columnas se trae aparte, con
            // HydrateAsync, que sí puede usar NOLOCK porque ya no compite por locks con nadie.
            //
            // Por qué el claim en sí ya no puede ser NOLOCK como el SELECT original: es una
            // escritura real, y eso es justamente lo que evita que dos workers procesen la
            // misma fila (ver el resto de comentarios de este archivo sobre el bug de
            // duplicados). READPAST evita que este UPDATE se bloquee contra otras filas en
            // vuelo — no evita que otros se bloqueen contra él, por eso el OUTPUT liviano.
            //
            // Sin DbRetry a propósito: este UPDATE no es idempotente. Si commitea del lado
            // del server pero el cliente nunca llega a leer las filas de OUTPUT (ej. se
            // corta la conexión a mitad de la transmisión del resultado), un reintento
            // automático con el mismo WHERE ya no matchea ese lote — lo reclamó el intento
            // anterior — y termina reclamando un lote distinto. Los LastInteractionId del
            // primer lote nunca llegan a la app, así que nadie los libera: quedan en
            // Envio=2 por una simple carrera de red, no por un crash. Mejor dejar que la
            // excepción suba (el catch de más abajo la loguea) y que el próximo poll lo
            // intente de nuevo — sin reintento automático ciego acá.
            const string claimSql = @"
                DECLARE @TargetDate DATE = COALESCE(@ReprocessDate, CAST(DATEADD(HOUR, -5, GETDATE()) AS DATE));
                UPDATE TOP (@Top) GSS_OraclePacifico WITH(ROWLOCK, READPAST)
                SET EnvioIniLLamada = 2, FechaReserva = GETDATE()
                OUTPUT INSERTED.LastInteractionId
                WHERE EnvioIniLLamada = 0
                    AND FechaIniLLamada >= @TargetDate
                    AND FechaIniLLamada <  DATEADD(DAY, 1, @TargetDate)";

            List<string> claimedIds;
            try
            {
                using var connection = _context.CreateConnection();
                connection.Open();
                var result = await connection.QueryAsync<string>(claimSql, new { Top = top, ReprocessDate = GetReprocessDate() }, commandType: CommandType.Text);
                claimedIds = result.ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PopulateIniLLamada falló (sin reintento automático — el UPDATE de claim no es idempotente)");
                throw;
            }

            try
            {
                return await HydrateAsync(claimedIds);
            }
            catch (Exception ex)
            {
                // Acá el claim ya commiteó — estas filas quedaron en Envio=2 sin que el
                // resto de sus datos haya llegado a la app. Loguear los IDs explícitamente
                // para que sean visibles en el triage manual, no un huérfano silencioso.
                _logger.LogError(ex, "PopulateIniLLamada: el claim de {Count} filas commiteó pero la hidratación falló — quedan en Envio=2 sin liberar: {Ids}", claimedIds.Count, string.Join(",", claimedIds));
                throw;
            }
        }

        public async Task<IReadOnlyList<CtiInteraccion>> PopulateFinLLamada(int top)
        {
            // Mismo ajuste de reloj, mismo claim en dos pasos y mismo motivo para no
            // envolver esto en DbRetry que PopulateIniLLamada — ver comentarios ahí.
            const string claimSql = @"
                DECLARE @TargetDate DATE = COALESCE(@ReprocessDate, CAST(DATEADD(HOUR, -5, GETDATE()) AS DATE));
                UPDATE TOP (@Top) GSS_OraclePacifico WITH(ROWLOCK, READPAST)
                SET EnvioFinLLamada = 2, FechaReserva = GETDATE()
                OUTPUT INSERTED.LastInteractionId
                WHERE EnvioFinLLamada = 0
                    AND EnvioIniLLamada = 1
                    AND FechaFinLLamada IS NOT NULL
                    AND FechaIniLLamada >= @TargetDate
                    AND FechaIniLLamada <  DATEADD(DAY, 1, @TargetDate)";

            List<string> claimedIds;
            try
            {
                using var connection = _context.CreateConnection();
                connection.Open();
                var result = await connection.QueryAsync<string>(claimSql, new { Top = top, ReprocessDate = GetReprocessDate() }, commandType: CommandType.Text);
                claimedIds = result.ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PopulateFinLLamada falló (sin reintento automático — el UPDATE de claim no es idempotente)");
                throw;
            }

            try
            {
                return await HydrateAsync(claimedIds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PopulateFinLLamada: el claim de {Count} filas commiteó pero la hidratación falló — quedan en Envio=2 sin liberar: {Ids}", claimedIds.Count, string.Join(",", claimedIds));
                throw;
            }
        }

        // Trae el resto de las columnas para un lote ya reclamado. WITH(NOLOCK) acá es
        // seguro (y deseable): estas filas ya cambiaron de estado en el paso de claim,
        // nadie más las va a tocar mientras están en Envio=2, así que no hay necesidad
        // de esperar ni tomar locks para leerlas — igual que el SELECT original, antes
        // de que existiera el claim.
        private async Task<IReadOnlyList<CtiInteraccion>> HydrateAsync(IReadOnlyList<string> lastInteractionIds)
        {
            if (lastInteractionIds.Count == 0)
                return Array.Empty<CtiInteraccion>();

            const string sql = @"
                SELECT LastInteractionId, Celular, Proveedor, FechaIniLLamada,
                       Tipo, AgenteId, JsonIni, RespuestaIni, IdOracle, UrlOracle,
                       EnvioIniLLamada, FechaFinLLamada, JsonFin, RespuestaFin,
                       EnvioFinLLamada, FechaRegistro, IdOportunidad, Resultado, Motivo
                FROM GSS_OraclePacifico WITH(NOLOCK)
                WHERE LastInteractionId IN @Ids";

            using var connection = _context.CreateConnection();
            connection.Open();
            var result = await connection.QueryAsync<CtiInteraccion>(sql, new { Ids = lastInteractionIds }, commandType: CommandType.Text);
            return result.ToList();
        }

        public async Task<bool> UpdateIniLLamada(string jsonIni, string jsonRespuestaIni, int envioIniLLamada, string lastInteractionId, long idOracle, string urlOracle)
        {
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET JsonIni         = @JsonIni,
                    RespuestaIni    = @JsonRespuestaIni,
                    EnvioIniLLamada = @EnvioIniLLamada,
                    IdOracle        = @IdOracle,
                    UrlOracle       = @UrlOracle
                WHERE LastInteractionId = @LastInteractionId";

            var p = new DynamicParameters();
            p.Add("@JsonIni", jsonIni, DbType.String);
            p.Add("@JsonRespuestaIni", jsonRespuestaIni, DbType.String);
            p.Add("@EnvioIniLLamada", envioIniLLamada, DbType.Int32);
            p.Add("@IdOracle", idOracle, DbType.Int64);
            p.Add("@UrlOracle", urlOracle, DbType.String);
            p.Add("@LastInteractionId", lastInteractionId, DbType.String);

            try
            {
                // PersistConfirmedResultPolicy en vez de DbRetry a propósito: esto persiste
                // un resultado que Oracle ya confirmó (o rechazó definitivamente) — no es un
                // dato cualquiera que se puede perder tras 2 intentos cortos. Ver comentarios
                // en ResiliencePolicies.cs.
                return await ResiliencePolicies.PersistConfirmedResultPolicy.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, p) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "UpdateIniLLamada falló tras agotar reintentos y circuit breaker para {LastInteractionId}", lastInteractionId);
                throw;
            }
        }

        public async Task<bool> UpdateFinLLamada(string jsonFin, string jsonRespuestaFin, int envioFinLLamada, string lastInteractionId)
        {
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET JsonFin         = @JsonFin,
                    RespuestaFin    = @JsonRespuestaFin,
                    EnvioFinLLamada = @EnvioFinLLamada
                WHERE LastInteractionId = @LastInteractionId";

            var p = new DynamicParameters();
            p.Add("@JsonFin", jsonFin, DbType.String);
            p.Add("@JsonRespuestaFin", jsonRespuestaFin, DbType.String);
            p.Add("@EnvioFinLLamada", envioFinLLamada, DbType.Int32);
            p.Add("@LastInteractionId", lastInteractionId, DbType.String);

            try
            {
                // PersistConfirmedResultPolicy en vez de DbRetry — mismo motivo que
                // UpdateIniLLamada, ver comentario ahí.
                return await ResiliencePolicies.PersistConfirmedResultPolicy.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, p) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "UpdateFinLLamada falló tras agotar reintentos y circuit breaker para {LastInteractionId}", lastInteractionId);
                throw;
            }
        }

        public async Task<bool> ReleaseIniLLamadaClaim(string lastInteractionId)
        {
            // Revierte una fila reservada (Envio = 2) a pendiente (0) cuando el intento
            // fue un TransientFailure — este es el ciclo normal de reintento, no el caso
            // huérfano. El "AND EnvioIniLLamada = 2" es defensivo: si por algún motivo la
            // fila ya no está en ese estado, no la pisa.
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET EnvioIniLLamada = 0
                WHERE LastInteractionId = @LastInteractionId AND EnvioIniLLamada = 2";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, new { LastInteractionId = lastInteractionId }) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ReleaseIniLLamadaClaim falló después de 2 reintentos para {LastInteractionId}", lastInteractionId);
                throw;
            }
        }

        public async Task<bool> ReleaseFinLLamadaClaim(string lastInteractionId)
        {
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET EnvioFinLLamada = 0
                WHERE LastInteractionId = @LastInteractionId AND EnvioFinLLamada = 2";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, new { LastInteractionId = lastInteractionId }) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ReleaseFinLLamadaClaim falló después de 2 reintentos para {LastInteractionId}", lastInteractionId);
                throw;
            }
        }

        public async Task<bool> MarkIniLLamadaConfirmedUnpersisted(string lastInteractionId)
        {
            // Último recurso cuando UpdateIniLLamada ya agotó PersistConfirmedResultPolicy
            // — incluido su circuit breaker, que a esta altura probablemente está abierto.
            // Por eso esto usa DbRetry liviano en vez de la misma política: si se reusara
            // PersistConfirmedResultPolicy, este intento fallaría instantáneo por el
            // circuito ya abierto de la operación pesada, sin ni siquiera probar. Un UPDATE
            // de una sola columna tiene más chance de entrar que el UPDATE completo que
            // acaba de fallar. Si aun así falla, la fila queda en Envio=2 sin distinguir del
            // huérfano genérico — caso raro dentro de un caso raro, aceptado.
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET EnvioIniLLamada = 4
                WHERE LastInteractionId = @LastInteractionId AND EnvioIniLLamada = 2";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, new { LastInteractionId = lastInteractionId }) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MarkIniLLamadaConfirmedUnpersisted falló para {LastInteractionId} — la fila queda en Envio=2 sin distinguir del huérfano genérico", lastInteractionId);
                return false;
            }
        }

        public async Task<bool> MarkFinLLamadaConfirmedUnpersisted(string lastInteractionId)
        {
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET EnvioFinLLamada = 4
                WHERE LastInteractionId = @LastInteractionId AND EnvioFinLLamada = 2";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, new { LastInteractionId = lastInteractionId }) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MarkFinLLamadaConfirmedUnpersisted falló para {LastInteractionId} — la fila queda en Envio=2 sin distinguir del huérfano genérico", lastInteractionId);
                return false;
            }
        }

        public async Task<int> ReclaimOrphanedIniLLamada(int timeoutMinutes)
        {
            // Filas en Envio=2 son, por construcción, las que nunca llegaron a un resultado
            // confirmado de Oracle — el caso confirmado-pero-no-persistido escribe Envio=4,
            // no 2 (ver MarkIniLLamadaConfirmedUnpersisted). Por eso acá es seguro devolver
            // a 0 sin más preguntas: nunca puede tratarse de una gestión que Oracle ya creó.
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET EnvioIniLLamada = 0
                WHERE EnvioIniLLamada = 2
                    AND FechaReserva IS NOT NULL
                    AND FechaReserva < DATEADD(MINUTE, -@TimeoutMinutes, GETDATE())";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<int>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, new { TimeoutMinutes = timeoutMinutes });
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ReclaimOrphanedIniLLamada falló después de 2 reintentos");
                throw;
            }
        }

        public async Task<int> ReclaimOrphanedFinLLamada(int timeoutMinutes)
        {
            const string sql = @"
                UPDATE GSS_OraclePacifico
                SET EnvioFinLLamada = 0
                WHERE EnvioFinLLamada = 2
                    AND FechaReserva IS NOT NULL
                    AND FechaReserva < DATEADD(MINUTE, -@TimeoutMinutes, GETDATE())";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<int>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, new { TimeoutMinutes = timeoutMinutes });
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ReclaimOrphanedFinLLamada falló después de 2 reintentos");
                throw;
            }
        }

        public async Task InsertMachineOracle()
        {
            try
            {
                await ResiliencePolicies.DbRetry.ExecuteAsync(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    await connection.ExecuteAsync("SppGss_App_InsertMachineOracle", commandType: CommandType.StoredProcedure);
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "InsertMachineOracle falló después de 2 reintentos");
                throw;
            }
        }
    }
}
