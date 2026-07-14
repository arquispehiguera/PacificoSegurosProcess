using Dapper;
using Microsoft.Extensions.Logging;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;
using PacificoSeguros.Infraestructure.Data;
using System.Data;

namespace PacificoSeguros.Infraestructure.Repositories
{
    public class InteraccionRepository : IInteraccionRepository
    {
        private readonly DbContextApp _context;
        private readonly ILogger<InteraccionRepository> _logger;

        public InteraccionRepository(DbContextApp context, ILogger<InteraccionRepository> logger)
        {
            _context = context;
            _logger = logger;
        }

        public async Task<IReadOnlyList<CtiInteraccion>> PopulateIniLLamada()
        {
            // GETDATE() en este SQL Server corre ~5 horas adelantado respecto a la hora
            // local del negocio (confirmado con SELECT GETDATE() en producción). Sin el
            // DATEADD(HOUR,-5,...), cualquier registro creado entre ~19hs y medianoche
            // hora local quedaría permanentemente fuera del filtro de "hoy" — el server
            // ya cree que es el día siguiente. Si algún día se corrige el reloj del
            // server, este ajuste hay que sacarlo.
            const string sql = @"
                SELECT LastInteractionId, Celular, Proveedor, FechaIniLLamada,
                       Tipo, AgenteId, JsonIni, RespuestaIni, IdOracle, UrlOracle,
                       EnvioIniLLamada, FechaFinLLamada, JsonFin, RespuestaFin,
                       EnvioFinLLamada, FechaRegistro, IdOportunidad
                FROM GSS_OraclePacifico WITH(NOLOCK)
                WHERE EnvioIniLLamada = 0
                    AND FechaIniLLamada >= CAST(DATEADD(HOUR, -5, GETDATE()) AS DATE)
                    AND FechaIniLLamada <  DATEADD(DAY, 1, CAST(DATEADD(HOUR, -5, GETDATE()) AS DATE))";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<IReadOnlyList<CtiInteraccion>>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    var result = await connection.QueryAsync<CtiInteraccion>(sql, commandType: CommandType.Text);
                    return result.ToList();
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PopulateIniLLamada falló después de 2 reintentos");
                throw;
            }
        }

        public async Task<IReadOnlyList<CtiInteraccion>> PopulateFinLLamada()
        {
            // Mismo ajuste de reloj que PopulateIniLLamada — ver comentario ahí.
            const string sql = @"
                SELECT LastInteractionId, Celular, Proveedor, FechaIniLLamada,
                       Tipo, AgenteId, JsonIni, RespuestaIni, IdOracle, UrlOracle,
                       EnvioIniLLamada, FechaFinLLamada, JsonFin, RespuestaFin,
                       EnvioFinLLamada, FechaRegistro, IdOportunidad
                FROM GSS_OraclePacifico WITH(NOLOCK)
                WHERE EnvioFinLLamada = 0
                    AND EnvioIniLLamada = 1
                    AND FechaFinLLamada IS NOT NULL
                    AND FechaIniLLamada >= CAST(DATEADD(HOUR, -5, GETDATE()) AS DATE)
                    AND FechaIniLLamada <  DATEADD(DAY, 1, CAST(DATEADD(HOUR, -5, GETDATE()) AS DATE))";

            try
            {
                return await ResiliencePolicies.DbRetry.ExecuteAsync<IReadOnlyList<CtiInteraccion>>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    var result = await connection.QueryAsync<CtiInteraccion>(sql, commandType: CommandType.Text);
                    return result.ToList();
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PopulateFinLLamada falló después de 2 reintentos");
                throw;
            }
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
                return await ResiliencePolicies.DbRetry.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, p) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "UpdateIniLLamada falló después de 2 reintentos para {LastInteractionId}", lastInteractionId);
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
                return await ResiliencePolicies.DbRetry.ExecuteAsync<bool>(async () =>
                {
                    using var connection = _context.CreateConnection();
                    connection.Open();
                    return await connection.ExecuteAsync(sql, p) > 0;
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "UpdateFinLLamada falló después de 2 reintentos para {LastInteractionId}", lastInteractionId);
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
