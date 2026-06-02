using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using PacificoSeguros.Core.Interfaces;
using PacificoSeguros.Infraestructure.Data;
using PacificoSeguros.Infraestructure.Repositories;
using PacificoSeguros.Infraestructure.Services;
using PacificoSeguros.Process.Services;

namespace SendDataRecruiting
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string basePath = Debugger.IsAttached
                ? AppContext.BaseDirectory
                : Environment.GetEnvironmentVariable("PACIFICO_BASE_PATH") ?? @"C:\JobsDeployment\PacificoSeguros";

            if (Debugger.IsAttached)
                Console.WriteLine($"Ejecutando en modo desarrollo: {basePath}");

            var configuration = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .Enrich.FromLogContext()
                .Enrich.WithProperty("Application", "PacificoSegurosProcess")
                .CreateLogger();

            while (true)
            {
                try
                {
                    Log.Information("Iniciando CtiInteraccion Processor...");
                    var host = CreateHostBuilder(args, configuration).Build();
                    await host.RunAsync();
                    Log.Information("Aplicación detenida correctamente");
                    break;
                }
                catch (Exception ex)
                {
                    Log.Fatal(ex, "Error fatal — reiniciando en 30 segundos...");
                    await Task.Delay(TimeSpan.FromSeconds(30));
                    Log.Information("Reiniciando aplicación...");
                }
            }

            Log.CloseAndFlush();
        }

        private static IHostBuilder CreateHostBuilder(string[] args, IConfiguration configuration) =>
            Host.CreateDefaultBuilder(args)
                .UseSerilog()
                .ConfigureAppConfiguration((_, builder) => builder.AddConfiguration(configuration))
                .ConfigureServices((_, services) =>
                {
                    services.AddSingleton<DbContextApp>();
                    services.AddHttpClient();
                    services.AddTransient<IInteraccionRepository, InteraccionRepository>();
                    services.AddSingleton<IOracleApiClient, OracleApiClient>();
                    services.AddHostedService<CtiInteraccionBackgroundService>();
                });
    }
}
