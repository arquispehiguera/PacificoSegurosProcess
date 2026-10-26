using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data.Common;

namespace PacificoSeguros.Infraestructure.Data
{
    public class DbContextApp
    {
        private readonly string _connectionString;
        public DbContextApp(IConfiguration configuration)
        {
            _connectionString = configuration.GetSection("ConnectionStrings")["AppConnection"]
                ?? throw new ArgumentNullException("ConnectionStrings:AppConnection", "La cadena de conexión no está configurada.");
        }

        // DbConnection en vez de IDbConnection: expone OpenAsync. El Open() sincrónico
        // bloquea un hilo del pool durante todo el timeout de conexión cuando SQL no es
        // alcanzable (error 40 / Win32 53), y con varios workers concurrentes eso
        // satura el pool justo en el momento de la caída.
        public DbConnection CreateConnection() => new SqlConnection(_connectionString);
    }
}
