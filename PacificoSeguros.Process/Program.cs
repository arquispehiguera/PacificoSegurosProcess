using System.Diagnostics;
using System.Net.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using PacificoSeguros.Core.Interfaces;
using PacificoSeguros.Infraestructure.Data;
using PacificoSeguros.Infraestructure.Repositories;
using PacificoSeguros.Infraestructure.Services;
using PacificoSeguros.Process.Services;

namespace PacificoSeguros.Process
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string basePath;
            if (Debugger.IsAttached)
            {
                basePath = AppContext.BaseDirectory;
                Console.WriteLine($"🧩 Ejecutando en modo desarrollo: {basePath}");
            }
            else
            {
                basePath = @"C:\JobsDeployment\PacificoSegurosProcess";
            }

            // Serilog.Sinks.File resuelve rutas relativas contra el directorio de
            // trabajo del proceso — un Windows Service no necesariamente arranca con
            // un CWD útil (puede ser C:\Windows\System32). Fijarlo acá reusa el mismo
            // basePath ya calculado, sin duplicar la lógica dev/prod en appsettings.json.
            Environment.CurrentDirectory = basePath;

            var configuration = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .Enrich.FromLogContext()
                .Enrich.WithProperty("Application", "PacificoSegurosProcess")
                .CreateLogger();

            try
            {
                Log.Information("Iniciando CtiInteraccion Processor...");
                var host = CreateHostBuilder(args, configuration).Build();
                await host.RunAsync();
                Log.Information("Aplicación detenida correctamente");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Error fatal al iniciar o ejecutar el host");
                Log.CloseAndFlush();
                // Código de salida != 0: señal inequívoca para el SCM de que hay que
                // aplicar la política de Recovery — no reintentamos acá adentro.
                Environment.Exit(1);
            }

            Log.CloseAndFlush();
        }

        private static IHostBuilder CreateHostBuilder(string[] args, IConfiguration configuration) =>
            Host.CreateDefaultBuilder(args)
                .UseWindowsService(options => options.ServiceName = "PacificoSegurosProcess")
                .UseSerilog()
                .ConfigureAppConfiguration((_, builder) => builder.AddConfiguration(configuration))
                .ConfigureServices((_, services) =>
                {
                    services.Configure<HostOptions>(o =>
                    {
                        o.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.StopHost;
                        o.ShutdownTimeout = TimeSpan.FromSeconds(15);
                    });

                    services.AddSingleton<DbContextApp>();
                    services.AddHttpClient();
                    services.ConfigureHttpClientDefaults(builder =>
                        builder.ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
                        {
                            PooledConnectionLifetime = TimeSpan.FromMinutes(5)
                        }));
                    services.AddTransient<IInteraccionRepository, InteraccionRepository>();
                    services.AddSingleton<IOracleApiClient, OracleApiClient>();
                    services.AddKeyedSingleton<IHeartbeatMonitor, HeartbeatMonitor>("Producer");
                    services.AddKeyedSingleton<IHeartbeatMonitor, HeartbeatMonitor>("Consumers");
                    services.AddHostedService<CtiInteraccionBackgroundService>();
                    services.AddHostedService<WatchdogHostedService>();
                });
    }
}
