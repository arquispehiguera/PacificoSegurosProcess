using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

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
        public IDbConnection CreateConnection() => new SqlConnection(_connectionString);
    }
}
