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
        private readonly int _batchSize;
        private readonly int _claimTimeoutMinutes;

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
            // Tamaño de tanda por poll — acota cuántas filas quedan reservadas (Envio = 2)
            // en RAM a la vez. No define el throughput real contra Oracle: eso lo hace el
            // rate limiter de OracleApiClient (100/min), que aplica sin importar cuánto
            // traiga acá. Este valor solo evita reservar de más entre un poll y el siguiente.
            _batchSize = configuration.GetValue<int>("OracleApi:BatchSize", 20);
            // Umbral para dar por huérfana una fila en Envio=2 y devolverla a 0. Con margen
            // sobre el peor caso razonable (una fila esperando turno en el Channel durante
            // un backlog grande), no sobre el caso típico (que resuelve en segundos). Nunca
            // reclama una fila en Envio=4 — ese estado significa que Oracle ya confirmó y
            // es intocable por diseño.
            _claimTimeoutMinutes = configuration.GetValue<int>("OracleApi:ClaimTimeoutMinutes", 20);

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
                    var iniRecords = await repo.PopulateIniLLamada(_batchSize);
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
                            // Try/catch por registro: iniRecords ya salió de un UPDATE de
                            // claim (Envio=2 para todo el lote). Si un registro revienta acá
                            // y no hay try/catch propio, el catch del ciclo completo aborta
                            // el foreach y el resto del lote ya reservado queda huérfano en
                            // Envio=2 sin necesidad — un solo registro problemático no debería
                            // abandonar a los demás que ya están listos para procesarse.
                            try
                            {
                                _logger.LogInformation("[DEBUG] Procesando IniLLamada: {LI}", record.LastInteractionId);
                                _ = await ProcessIniLlamada(repo, record, ct);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "[DEBUG] Error procesando IniLLamada {LI}", record.LastInteractionId);
                            }
                        }
                    }

                    var finRecords = await repo.PopulateFinLLamada(_batchSize);
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
                            try
                            {
                                _logger.LogInformation("[DEBUG] Procesando FinLLamada: {LI}", record.LastInteractionId);
                                _ = await ProcessFinLlamada(repo, record, ct);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "[DEBUG] Error procesando FinLLamada {LI}", record.LastInteractionId);
                            }
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

                    // Devuelve a Envio=0 las filas reservadas hace más de _claimTimeoutMinutes
                    // sin resolver — nunca toca Envio=4 (Oracle ya confirmó, es intocable).
                    // Va antes del Populate del ciclo para que lo recién liberado pueda
                    // reclamarse en el mismo ciclo, no en el siguiente.
                    var reclaimedIni = await repo.ReclaimOrphanedIniLLamada(_claimTimeoutMinutes);
                    if (reclaimedIni > 0)
                        _logger.LogWarning("{Count} interacciones IniLLamada reclamadas por timeout (más de {TimeoutMin}min en Envio=2)", reclaimedIni, _claimTimeoutMinutes);
                    var reclaimedFin = await repo.ReclaimOrphanedFinLLamada(_claimTimeoutMinutes);
                    if (reclaimedFin > 0)
                        _logger.LogWarning("{Count} interacciones FinLLamada reclamadas por timeout (más de {TimeoutMin}min en Envio=2)", reclaimedFin, _claimTimeoutMinutes);

                    var iniRecords = await repo.PopulateIniLLamada(_batchSize);
                    if (iniRecords.Any())
                    {
                        _logger.LogInformation("{Count} interacciones IniLLamada pendientes", iniRecords.Count);
                        foreach (var r in iniRecords)
                            await _iniChannel.Writer.WriteAsync(r, ct);
                    }
                    var finRecords = await repo.PopulateFinLLamada(_batchSize);
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
                            var outcome = await ProcessIniLlamada(repo, record, ct);
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
                            var outcome = await ProcessFinLlamada(repo, record, ct);
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

        private async Task<ApiOutcome> ProcessIniLlamada(IInteraccionRepository repo, CtiInteraccion record, CancellationToken ct)
        {
            var request = new OracleIniLlamadaRequest
            {
                tANI_c = record.Celular,
                tProveedor_c = record.Proveedor,
                dInicio_c = FormatFecha(record.FechaIniLLamada),
                tUCID_c = record.LastInteractionId,
                chTipo_c = record.Tipo,
                chOpty_Id_c = record.IdOportunidad,
                tUsuarioNumDoc_c = record.AgenteId,
                chOptyTipifResultado_c = record.Resultado,
                chOptyTipifSubResultado_c = record.Motivo
            };

            var (outcome, response) = await _oracleClient.IniciarGestionAsync(request, ct);

            if (outcome == ApiOutcome.TransientFailure)
            {
                // La fila quedó reservada (Envio=2) por el claim del Populate — hay que
                // devolverla a 0 explícitamente para que el próximo ciclo la vuelva a
                // tomar. Distinto del huérfano por crash/shutdown: acá el proceso sigue
                // vivo y sabe que este intento no llegó a destino.
                await TryReleaseClaim(() => repo.ReleaseIniLLamadaClaim(record.LastInteractionId!), record.LastInteractionId!);
                return outcome;
            }

            int envio = outcome == ApiOutcome.Success && response?.Id > 0 ? 1 : 3;
            long idOracle = response?.Id ?? 0;
            string urlOracle = response?.tURL_c ?? "";

            try
            {
                await repo.UpdateIniLLamada(
                    JsonConvert.SerializeObject(request),
                    JsonConvert.SerializeObject(response),
                    envio,
                    record.LastInteractionId!,
                    idOracle,
                    urlOracle);
            }
            catch (Exception ex)
            {
                // UpdateIniLLamada ya agotó su propia política de reintento + circuit
                // breaker (PersistConfirmedResultPolicy) antes de llegar acá. Si el outcome
                // era Success, Oracle ya creó la gestión — esta fila, si se queda en Envio=2,
                // es indistinguible de un huérfano que nunca tocó a Oracle, y el timeout de
                // reclamo la reenviaría creando una gestión duplicada. Por eso el intento
                // final: marcarla en Envio=4, un estado que el reclamo por timeout nunca
                // toca. Si ni ese UPDATE liviano logra entrar, al menos queda el log
                // explícito con el IdOracle para el triage manual.
                if (outcome == ApiOutcome.Success)
                {
                    var marked = await repo.MarkIniLLamadaConfirmedUnpersisted(record.LastInteractionId!);
                    if (marked)
                        _logger.LogError(ex, "No se pudo persistir el éxito de {LI} — Oracle ya procesó esta interacción (IdOracle={IdOracle}, UrlOracle={UrlOracle}). Marcada en Envio=4, no se reintentará sola: requiere corrección manual (UPDATE Envio=1, IdOracle, UrlOracle) en GSS_OraclePacifico.", record.LastInteractionId, idOracle, urlOracle);
                    else
                        _logger.LogError(ex, "No se pudo persistir el éxito de {LI} ni marcarla en Envio=4 — Oracle ya procesó esta interacción (IdOracle={IdOracle}, UrlOracle={UrlOracle}). La fila queda en Envio=2: el reclamo por timeout la va a reenviar si no se corrige a mano antes.", record.LastInteractionId, idOracle, urlOracle);
                }
                else
                {
                    _logger.LogError(ex, "No se pudo persistir el resultado (PermanentFailure) de {LI} — la fila queda en Envio=2, elegible para reclamo por timeout.", record.LastInteractionId);
                }
            }

            return outcome;
        }

        private async Task<ApiOutcome> ProcessFinLlamada(IInteraccionRepository repo, CtiInteraccion record, CancellationToken ct)
        {
            var request = new OracleFinLlamadaRequest
            {
                dFin_c = FormatFecha(record.FechaFinLLamada),
                chOptyTipifResultado_c = record.Resultado,
                chOptyTipifSubResultado_c = record.Motivo
            };
            var (outcome, responseBody) = await _oracleClient.FinalizarGestionAsync(request, record.IdOracle ?? 0L, ct);

            if (outcome == ApiOutcome.TransientFailure)
            {
                await TryReleaseClaim(() => repo.ReleaseFinLLamadaClaim(record.LastInteractionId!), record.LastInteractionId!);
                return outcome;
            }

            try
            {
                await repo.UpdateFinLLamada(
                    JsonConvert.SerializeObject(request),
                    responseBody ?? string.Empty,
                    outcome == ApiOutcome.Success ? 1 : 3,
                    record.LastInteractionId!);
            }
            catch (Exception ex)
            {
                // Mismo motivo que en ProcessIniLlamada — UpdateFinLLamada ya agotó
                // PersistConfirmedResultPolicy. record.IdOracle ya viene seteado del paso
                // Ini previo, es el identificador que ata esta fila a la gestión en Oracle.
                if (outcome == ApiOutcome.Success)
                {
                    var marked = await repo.MarkFinLLamadaConfirmedUnpersisted(record.LastInteractionId!);
                    if (marked)
                        _logger.LogError(ex, "No se pudo persistir el éxito de FinalizarGestion en {LI} — Oracle ya procesó esta finalización (IdOracle={IdOracle}). Marcada en Envio=4, no se reintentará sola: requiere corrección manual (UPDATE EnvioFinLLamada=1) en GSS_OraclePacifico.", record.LastInteractionId, record.IdOracle);
                    else
                        _logger.LogError(ex, "No se pudo persistir el éxito de FinalizarGestion en {LI} ni marcarla en Envio=4 — Oracle ya procesó esta finalización (IdOracle={IdOracle}). La fila queda en Envio=2: el reclamo por timeout la va a reenviar si no se corrige a mano antes.", record.LastInteractionId, record.IdOracle);
                }
                else
                {
                    _logger.LogError(ex, "No se pudo persistir el resultado (PermanentFailure) de FinalizarGestion en {LI} — la fila queda en Envio=2, elegible para reclamo por timeout.", record.LastInteractionId);
                }
            }

            return outcome;
        }

        // Libera el claim (Envio 2->0) tras un TransientFailure. Atrapa la excepción acá
        // en vez de dejarla escapar al catch genérico del worker: si la liberación misma
        // falla (ej. un lock transitorio en la base justo en ese instante), la fila queda
        // en Envio=2 igual — pero con este log específico se puede distinguir del huérfano
        // por crash/shutdown que el cliente aceptó como caso válido. También avisa si la
        // UPDATE no afectó ninguna fila (el claim ya no estaba en Envio=2 por otra razón).
        private async Task TryReleaseClaim(Func<Task<bool>> release, string lastInteractionId)
        {
            try
            {
                var released = await release();
                if (!released)
                    _logger.LogWarning("Release de claim en {LI} no afectó ninguna fila — el claim ya no estaba en Envio=2", lastInteractionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "No se pudo liberar el claim de {LI} tras un TransientFailure — la fila puede quedar en Envio=2 por esta falla, no por un crash/shutdown", lastInteractionId);
            }
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
