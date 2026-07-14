using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Serilog.Context;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;

namespace PacificoSeguros.Process.Services
{
    public class CtiInteraccionBackgroundService : BackgroundService
    {
        private readonly ILogger<CtiInteraccionBackgroundService> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConfiguration _configuration;
        private readonly IOracleApiClient _oracleClient;
        private readonly IHeartbeatMonitor _producerHeartbeat;
        private readonly IHeartbeatMonitor _consumersHeartbeat;

        private readonly Channel<CtiInteraccion> _iniChannel;
        private readonly Channel<CtiInteraccion> _finChannel;

        private readonly int _pollingIntervalSeconds;
        private readonly int _maxConcurrentWorkers;

        public CtiInteraccionBackgroundService(
            ILogger<CtiInteraccionBackgroundService> logger,
            IServiceProvider serviceProvider,
            IConfiguration configuration,
            IOracleApiClient oracleClient,
            [FromKeyedServices("Producer")] IHeartbeatMonitor producerHeartbeat,
            [FromKeyedServices("Consumers")] IHeartbeatMonitor consumersHeartbeat)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _configuration = configuration;
            _oracleClient = oracleClient;
            _producerHeartbeat = producerHeartbeat;
            _consumersHeartbeat = consumersHeartbeat;
            _pollingIntervalSeconds = configuration.GetValue<int>("OracleApi:PollingIntervalSeconds", 10);
            _maxConcurrentWorkers = configuration.GetValue<int>("OracleApi:MaxConcurrentWorkers", 5);

            _iniChannel = Channel.CreateUnbounded<CtiInteraccion>(new UnboundedChannelOptions { SingleWriter = true });
            _finChannel = Channel.CreateUnbounded<CtiInteraccion>(new UnboundedChannelOptions { SingleWriter = true });

            _logger.LogInformation("CtiInteraccionProcessor iniciado — Polling: {Interval}s, Workers: {Workers}",
                _pollingIntervalSeconds, _maxConcurrentWorkers);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (Debugger.IsAttached)
            {
                _logger.LogInformation("Modo DEBUG — ejecución secuencial paso a paso");
                await RunSequentialAsync(stoppingToken);
            }
            else
            {
                _logger.LogInformation("Modo PRODUCCIÓN — workers en paralelo");
                var producer = ProducerTask(stoppingToken);
                var iniWorkers = Enumerable.Range(1, _maxConcurrentWorkers).Select(id => IniConsumerTask(id, stoppingToken));
                var finWorkers = Enumerable.Range(1, _maxConcurrentWorkers).Select(id => FinConsumerTask(id, stoppingToken));
                await Task.WhenAll(new[] { producer }.Concat(iniWorkers).Concat(finWorkers));
            }
        }

        private async Task RunSequentialAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var repo = scope.ServiceProvider.GetRequiredService<IInteraccionRepository>();
                    await repo.InsertMachineOracle();
                    var iniRecords = await repo.PopulateIniLLamada();
                    _logger.LogInformation("[DEBUG] {Count} registros IniLLamada encontrados", iniRecords.Count);
                    foreach (var record in iniRecords)
                    {
                        // La columna de BD se llama "ContactId" porque otra aplicación distinta ya lee
                        // de esa misma columna en GSS_LogPacifico — no se puede renombrar sin
                        // romperle la lectura. Reusamos la columna, mandamos LastInteractionId
                        // como valor (es la clave que de verdad importa para correlacionar).
                        using (LogContext.PushProperty("Celular", MaskCelular(record.Celular)))
                        using (LogContext.PushProperty("ContactId", record.LastInteractionId))
                        {
                            _logger.LogInformation("[DEBUG] Procesando IniLLamada: {LI}", record.LastInteractionId);
                            _ = await ProcessIniLlamada(repo, record);
                        }
                    }

                    var finRecords = await repo.PopulateFinLLamada();
                    _logger.LogInformation("[DEBUG] {Count} registros FinLLamada encontrados", finRecords.Count);
                    foreach (var record in finRecords)
                    {
                        // La columna de BD se llama "ContactId" porque otra aplicación distinta ya lee
                        // de esa misma columna en GSS_LogPacifico — no se puede renombrar sin
                        // romperle la lectura. Reusamos la columna, mandamos LastInteractionId
                        // como valor (es la clave que de verdad importa para correlacionar).
                        using (LogContext.PushProperty("Celular", MaskCelular(record.Celular)))
                        using (LogContext.PushProperty("ContactId", record.LastInteractionId))
                        {
                            _logger.LogInformation("[DEBUG] Procesando FinLLamada: {LI}", record.LastInteractionId);
                            _ = await ProcessFinLlamada(repo, record);
                        }
                    }

                    _producerHeartbeat.ReportAlive();
                    _producerHeartbeat.ReportProgress();
                    _consumersHeartbeat.ReportAlive();
                    _consumersHeartbeat.ReportProgress();
                    await Task.Delay(TimeSpan.FromSeconds(_pollingIntervalSeconds), ct);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[DEBUG] Error en ejecución secuencial");
                    await Task.Delay(TimeSpan.FromSeconds(_pollingIntervalSeconds), ct);
                }
            }
        }

        private async Task ProducerTask(CancellationToken ct)
        {
            _logger.LogInformation("Producer iniciado — polling cada {Interval}s", _pollingIntervalSeconds);
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var repo = scope.ServiceProvider.GetRequiredService<IInteraccionRepository>();
                    await repo.InsertMachineOracle();
                    var iniRecords = await repo.PopulateIniLLamada();
                    if (iniRecords.Any())
                    {
                        _logger.LogInformation("{Count} interacciones IniLLamada pendientes", iniRecords.Count);
                        foreach (var r in iniRecords)
                            await _iniChannel.Writer.WriteAsync(r, ct);
                    }
                    var finRecords = await repo.PopulateFinLLamada();
                    if (finRecords.Any())
                    {
                        _logger.LogInformation("{Count} interacciones FinLLamada pendientes", finRecords.Count);
                        foreach (var r in finRecords)
                            await _finChannel.Writer.WriteAsync(r, ct);
                    }
                    _producerHeartbeat.ReportAlive();
                    _producerHeartbeat.ReportProgress();
                    await Task.Delay(TimeSpan.FromSeconds(_pollingIntervalSeconds), ct);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error en Producer — reintentando en {Interval}s", _pollingIntervalSeconds);
                    await Task.Delay(TimeSpan.FromSeconds(_pollingIntervalSeconds), ct);
                }
            }
            _iniChannel.Writer.Complete();
            _finChannel.Writer.Complete();
            _logger.LogInformation("Producer detenido");
        }

        // El await foreach de ReadAllAsync bloquea sin ejecutar el cuerpo mientras el
        // channel está vacío — con eso, ReportAlive()/ReportProgress() no se llaman
        // durante ventanas de baja actividad (de noche, fin de semana), y el Watchdog
        // termina reiniciando el proceso creyendo que está colgado cuando en realidad
        // no hay nada para procesar. Este intervalo hace que el worker "avise que sigue
        // vivo" aunque el channel esté vacío, sin necesidad de que llegue trabajo real.
        private static readonly TimeSpan IdleHeartbeatInterval = TimeSpan.FromSeconds(15);

        private async Task IniConsumerTask(int workerId, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                bool hasData;
                try
                {
                    var waitTask = _iniChannel.Reader.WaitToReadAsync(ct).AsTask();
                    var idleTask = Task.Delay(IdleHeartbeatInterval, ct);
                    if (await Task.WhenAny(waitTask, idleTask) == idleTask)
                    {
                        // Sin trabajo pendiente hace un rato — no es un hang, es que no
                        // hay nada para hacer. El worker sigue vivo y al día.
                        _consumersHeartbeat.ReportAlive();
                        _consumersHeartbeat.ReportProgress();
                        continue;
                    }
                    hasData = await waitTask;
                }
                catch (OperationCanceledException) { break; }

                if (!hasData) break; // Producer cerró el channel

                // El !ct.IsCancellationRequested acá es necesario: sin él, si el shutdown
                // arranca mientras hay ítems en cola, este loop los sigue drenando igual
                // y cada uno choca contra el IServiceProvider ya disposed del Host —
                // una ráfaga de ObjectDisposedException, uno por ítem pendiente, en vez
                // de cortar prolijo apenas se pide la cancelación.
                while (!ct.IsCancellationRequested && _iniChannel.Reader.TryRead(out var record))
                {
                    // La columna de BD se llama "ContactId" porque otra aplicación distinta ya lee
                    // de esa misma columna en GSS_LogPacifico — no se puede renombrar sin
                    // romperle la lectura. Reusamos la columna, mandamos LastInteractionId
                    // como valor (es la clave que de verdad importa para correlacionar).
                    using (LogContext.PushProperty("Celular", MaskCelular(record.Celular)))
                    using (LogContext.PushProperty("ContactId", record.LastInteractionId))
                    {
                        try
                        {
                            using var scope = _serviceProvider.CreateScope();
                            var repo = scope.ServiceProvider.GetRequiredService<IInteraccionRepository>();
                            var outcome = await ProcessIniLlamada(repo, record);
                            if (outcome == ApiOutcome.TransientFailure)
                            {
                                _logger.LogDebug("Worker-Ini #{Id}: falla transitoria en {LI}, se reintentará en el próximo ciclo", workerId, record.LastInteractionId);
                            }
                            else
                            {
                                // Success no loguea nada acá — es el caso esperado, la
                                // mayoría de los ítems, y loguearlo uno por uno satura el
                                // archivo sin aportar señal. PermanentFailure sí interesa
                                // ver (poco volumen, y OracleApiClient ya lo logueó con
                                // más detalle — esto solo correlaciona qué worker lo tomó).
                                if (outcome == ApiOutcome.PermanentFailure)
                                    _logger.LogInformation("Worker-Ini #{Id}: falla permanente en {LI}", workerId, record.LastInteractionId);
                                _consumersHeartbeat.ReportProgress();
                            }
                            _consumersHeartbeat.ReportAlive();
                        }
                        // El filtro distingue una falla real de negocio de un efecto
                        // colateral esperado del apagado (ej. ObjectDisposedException
                        // del IServiceProvider si el Host empezó a disponerse justo
                        // mientras este ítem se estaba procesando) — lo segundo no es
                        // un "falló", es el proceso cerrando prolijo, y no corresponde
                        // loguearlo como error.
                        catch (Exception ex) when (!ct.IsCancellationRequested)
                        {
                            _logger.LogError(ex, "Worker-Ini #{Id} falló en {LI}", workerId, record.LastInteractionId);
                        }
                        catch (Exception)
                        {
                            break;
                        }
                    }
                }
            }
        }

        private async Task FinConsumerTask(int workerId, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                bool hasData;
                try
                {
                    var waitTask = _finChannel.Reader.WaitToReadAsync(ct).AsTask();
                    var idleTask = Task.Delay(IdleHeartbeatInterval, ct);
                    if (await Task.WhenAny(waitTask, idleTask) == idleTask)
                    {
                        _consumersHeartbeat.ReportAlive();
                        _consumersHeartbeat.ReportProgress();
                        continue;
                    }
                    hasData = await waitTask;
                }
                catch (OperationCanceledException) { break; }

                if (!hasData) break;

                while (!ct.IsCancellationRequested && _finChannel.Reader.TryRead(out var record))
                {
                    // La columna de BD se llama "ContactId" porque otra aplicación distinta ya lee
                    // de esa misma columna en GSS_LogPacifico — no se puede renombrar sin
                    // romperle la lectura. Reusamos la columna, mandamos LastInteractionId
                    // como valor (es la clave que de verdad importa para correlacionar).
                    using (LogContext.PushProperty("Celular", MaskCelular(record.Celular)))
                    using (LogContext.PushProperty("ContactId", record.LastInteractionId))
                    {
                        try
                        {
                            using var scope = _serviceProvider.CreateScope();
                            var repo = scope.ServiceProvider.GetRequiredService<IInteraccionRepository>();
                            var outcome = await ProcessFinLlamada(repo, record);
                            if (outcome == ApiOutcome.TransientFailure)
                            {
                                _logger.LogDebug("Worker-Fin #{Id}: falla transitoria en {LI}, se reintentará en el próximo ciclo", workerId, record.LastInteractionId);
                            }
                            else
                            {
                                if (outcome == ApiOutcome.PermanentFailure)
                                    _logger.LogInformation("Worker-Fin #{Id}: falla permanente en {LI}", workerId, record.LastInteractionId);
                                _consumersHeartbeat.ReportProgress();
                            }
                            _consumersHeartbeat.ReportAlive();
                        }
                        catch (Exception ex) when (!ct.IsCancellationRequested)
                        {
                            _logger.LogError(ex, "Worker-Fin #{Id} falló en {LI}", workerId, record.LastInteractionId);
                        }
                        catch (Exception)
                        {
                            break;
                        }
                    }
                }
            }
        }

        private async Task<ApiOutcome> ProcessIniLlamada(IInteraccionRepository repo, CtiInteraccion record)
        {
            var request = new OracleIniLlamadaRequest
            {
                tANI_c = record.Celular,
                tProveedor_c = record.Proveedor,
                dInicio_c = FormatFecha(record.FechaIniLLamada),
                tUCID_c = record.LastInteractionId,
                chTipo_c = record.Tipo,
                chOpty_Id_c = record.IdOportunidad,
                tUsuarioNumDoc_c = record.AgenteId
            };

            var (outcome, response) = await _oracleClient.IniciarGestionAsync(request);

            if (outcome == ApiOutcome.TransientFailure)
                return outcome; // no se toca la fila, sigue en Envio=0 para el próximo ciclo

            int envio = outcome == ApiOutcome.Success && response?.Id > 0 ? 1 : 3;
            long idOracle = response?.Id ?? 0;
            string urlOracle = response?.tURL_c ?? "";

            await repo.UpdateIniLLamada(
                JsonConvert.SerializeObject(request),
                JsonConvert.SerializeObject(response),
                envio,
                record.LastInteractionId!,
                idOracle,
                urlOracle);

            return outcome;
        }

        private async Task<ApiOutcome> ProcessFinLlamada(IInteraccionRepository repo, CtiInteraccion record)
        {
            var request = new OracleFinLlamadaRequest { dFin_c = FormatFecha(record.FechaFinLLamada) };
            var (outcome, responseBody) = await _oracleClient.FinalizarGestionAsync(request, record.IdOracle ?? 0L);

            if (outcome == ApiOutcome.TransientFailure)
                return outcome;

            await repo.UpdateFinLLamada(
                JsonConvert.SerializeObject(request),
                responseBody ?? string.Empty,
                outcome == ApiOutcome.Success ? 1 : 3,
                record.LastInteractionId!);

            return outcome;
        }

        private static string FormatFecha(DateTime? fecha) =>
            fecha.HasValue ? fecha.Value.ToString("yyyy-MM-ddTHH:mm:ss.fff-05:00") : string.Empty;

        // El celular es PII y termina en GSS_LogPacifico y en el archivo de log en
        // disco — no hace falta el número completo para correlacionar/depurar, alcanza
        // con los últimos 4 dígitos.
        private static string MaskCelular(string? celular)
        {
            if (string.IsNullOrEmpty(celular))
                return string.Empty;
            return celular.Length <= 4 ? celular : $"***{celular[^4..]}";
        }
    }
}
