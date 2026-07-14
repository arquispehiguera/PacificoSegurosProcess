using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PacificoSeguros.Process.Services
{
    public class WatchdogHostedService : BackgroundService
    {
        private readonly IHeartbeatMonitor _producerHeartbeat;
        private readonly IHeartbeatMonitor _consumersHeartbeat;
        private readonly ILogger<WatchdogHostedService> _logger;
        private readonly IHostApplicationLifetime _lifetime;
        private readonly TimeSpan _staleThreshold;
        private readonly TimeSpan _checkInterval;
        private readonly TimeSpan _maxNoProgress;

        public WatchdogHostedService(
            [FromKeyedServices("Producer")] IHeartbeatMonitor producerHeartbeat,
            [FromKeyedServices("Consumers")] IHeartbeatMonitor consumersHeartbeat,
            ILogger<WatchdogHostedService> logger,
            IHostApplicationLifetime lifetime,
            IConfiguration configuration)
        {
            _producerHeartbeat = producerHeartbeat;
            _consumersHeartbeat = consumersHeartbeat;
            _logger = logger;
            _lifetime = lifetime;

            var pollingSeconds = configuration.GetValue<int>("OracleApi:PollingIntervalSeconds", 10);
            var staleMultiplier = configuration.GetValue<int>("Watchdog:StaleMultiplier", 6);
            // Piso independiente del polling, recalculado para este proyecto: el Producer
            // hace hasta 3 llamadas secuenciales a la BD por vuelta (InsertMachineOracle,
            // PopulateIniLLamada, PopulateFinLLamada), cada una envuelta en DbRetry
            // (Policy.WaitAndRetryAsync con 2 reintentos, 2s fijo de espera cada uno —
            // ResiliencePolicies.cs) contra un CommandTimeout EXPLÍCITO de 10s (ver
            // "Command Timeout=10" en la connection string de appsettings.json, no el
            // default de ADO.NET). Peor caso por llamada: 3 intentos x 10s + 2 esperas x 2s
            // = 34s. Si las 3 llamadas agotan reintentos en la misma vuelta: ~102s. 200s da
            // casi el doble de margen sobre ese piso sin quedar tan laxo como para no
            // detectar un hang real. Si algún día cambia el CommandTimeout o el delay de
            // DbRetry, este cálculo hay que rehacerlo — no copiar el número a ciegas.
            var minThresholdSeconds = configuration.GetValue<int>("Watchdog:MinStaleThresholdSeconds", 200);
            _staleThreshold = TimeSpan.FromSeconds(Math.Max(pollingSeconds * staleMultiplier, minThresholdSeconds));
            _checkInterval = TimeSpan.FromSeconds(Math.Max(30, pollingSeconds * 2));

            // Umbral separado del heartbeat: un TransientFailure (rate limit, timeout de
            // lock) cuenta como "vivo" a propósito, para no reiniciar por un blip de
            // segundos. Pero si Oracle/Azure está mal de forma SOSTENIDA, cada worker
            // puede seguir "intentando y fallando prolijamente" indefinidamente sin que
            // el heartbeat se venza nunca — el proceso queda latiendo sin lograr nada
            // real. Este umbral, mucho más largo, mide hace cuánto no hay un resultado
            // DEFINITIVO (Success o PermanentFailure) — eso sí detecta ese escenario.
            var maxNoProgressMinutes = configuration.GetValue<int>("Watchdog:MaxNoProgressMinutes", 15);
            _maxNoProgress = TimeSpan.FromMinutes(maxNoProgressMinutes);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogWarning(
                "Watchdog iniciado — chequeo cada {CheckInterval}, umbral de inactividad {Threshold}",
                _checkInterval, _staleThreshold);

            while (!stoppingToken.IsCancellationRequested)
            {
                try { await Task.Delay(_checkInterval, stoppingToken); }
                catch (OperationCanceledException) { break; }

                var elapsedProducer = DateTime.UtcNow - _producerHeartbeat.LastHeartbeatUtc;
                var elapsedConsumers = DateTime.UtcNow - _consumersHeartbeat.LastHeartbeatUtc;
                var elapsedProducerProgress = DateTime.UtcNow - _producerHeartbeat.LastProgressUtc;
                var elapsedConsumersProgress = DateTime.UtcNow - _consumersHeartbeat.LastProgressUtc;

                var heartbeatVencido = elapsedProducer > _staleThreshold || elapsedConsumers > _staleThreshold;
                var sinProgresoReal = elapsedProducerProgress > _maxNoProgress || elapsedConsumersProgress > _maxNoProgress;

                if (heartbeatVencido || sinProgresoReal)
                {
                    _logger.LogCritical(
                        "Watchdog: {Motivo} (Producer alive={ElapsedProducer} progress={ElapsedProducerProgress}, Consumers alive={ElapsedConsumers} progress={ElapsedConsumersProgress}, umbral heartbeat={Threshold}, umbral progreso={MaxNoProgress}) — solicitando apagado del host",
                        heartbeatVencido ? "heartbeat vencido" : "sin progreso real sostenido",
                        elapsedProducer, elapsedProducerProgress, elapsedConsumers, elapsedConsumersProgress, _staleThreshold, _maxNoProgress);

                    // No forzamos Environment.FailFast acá: pedimos un shutdown grácil vía
                    // IHostApplicationLifetime (acotado por HostOptions.ShutdownTimeout) para
                    // darle al trabajo en curso una chance real de terminar, y dejamos que
                    // Program.cs haga el único Log.CloseAndFlush() al final, cuando RunAsync()
                    // ya volvió y no quedan otros BackgroundServices escribiendo al logger
                    // compartido. Environment.ExitCode queda seteado para que el proceso
                    // termine con código != 0 y el SCM aplique la política de Recovery.
                    Environment.ExitCode = 1;
                    _lifetime.StopApplication();
                    return;
                }
            }
        }
    }
}
