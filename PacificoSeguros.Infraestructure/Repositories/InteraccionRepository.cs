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
            const string sql = @"
                SELECT LastInteractionId, ContactId, Celular, Proveedor, FechaIniLLamada,
                       Tipo, AgenteId, JsonIni, RespuestaIni, IdOracle, UrlOracle,
                       EnvioIniLLamada, FechaFinLLamada, JsonFin, RespuestaFin,
                       EnvioFinLLamada, FechaRegistro
                FROM GSS_OraclePacifico WITH(NOLOCK)
                WHERE EnvioIniLLamada = 0";
            try
            {
                using var connection = _context.CreateConnection();
                connection.Open();
                var result = await connection.QueryAsync<CtiInteraccion>(sql, commandType: CommandType.Text);
                return result.ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al listar registros IniLLamada");
                throw;
            }
        }

        public async Task<IReadOnlyList<CtiInteraccion>> PopulateFinLLamada()
        {
            const string sql = @"
                SELECT LastInteractionId, ContactId, Celular, Proveedor, FechaIniLLamada,
                       Tipo, AgenteId, JsonIni, RespuestaIni, IdOracle, UrlOracle,
                       EnvioIniLLamada, FechaFinLLamada, JsonFin, RespuestaFin,
                       EnvioFinLLamada, FechaRegistro
                FROM GSS_OraclePacifico WITH(NOLOCK)
                WHERE EnvioFinLLamada = 0 AND EnvioIniLLamada = 1";
            try
            {
                using var connection = _context.CreateConnection();
                connection.Open();
                var result = await connection.QueryAsync<CtiInteraccion>(sql, commandType: CommandType.Text);
                return result.ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al listar registros FinLLamada");
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
                using var connection = _context.CreateConnection();
                connection.Open();
                return await connection.ExecuteAsync(sql, p) > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en UpdateIniLLamada para {LastInteractionId}", lastInteractionId);
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
                using var connection = _context.CreateConnection();
                connection.Open();
                return await connection.ExecuteAsync(sql, p) > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en UpdateFinLLamada para {LastInteractionId}", lastInteractionId);
                throw;
            }
        }
    }
}
