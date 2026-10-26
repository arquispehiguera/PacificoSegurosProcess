using System.Diagnostics;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Polly.CircuitBreaker;
using Serilog.Context;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;

namespace PacificoSeguros.Process.Services
{
    public class CtiInteraccionBackgroundService : BackgroundService
    {
        private readonly ILogger<CtiInteraccionBackgroundService> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IOracleApiClient _oracleClient;
        private readonly IHeartbeatMonitor _producerHeartbeat;
        private readonly IHeartbeatMonitor _consumersHeartbeat;
        private readonly IConfirmedResultOutbox _outbox;

        // Canales y pools separados por Origen — el otro aplicativo poll-ea la tabla
        // esperando Resultado/Motivo de AGENT en línea, así que AGENT no puede quedar
        // atrás de MACHINE bajo congestión. Ver también el rate limiter partido en
        // OracleApiClient (_agentRateLimiter/_machineRateLimiter).
        private readonly Channel<CtiInteraccion> _agentChannel;
        private readonly Channel<CtiInteraccion> _machineChannel;

        private readonly int _pollingIntervalSeconds;
        private readonly int _agentMaxConcurrentWorkers;
        private readonly int _machineMaxConcurrentWorkers;
        private readonly int _batchSize;
        private readonly int _claimTimeoutMinutes;
        private readonly TimeSpan _outboxExpireAfter;

        public CtiInteraccionBackgroundService(
            ILogger<CtiInteraccionBackgroundService> logger,
            IServiceProvider serviceProvider,
            IConfiguration configuration,
            IOracleApiClient oracleClient,
            [FromKeyedServices("Producer")] IHeartbeatMonitor producerHeartbeat,
            [FromKeyedServices("Consumers")] IHeartbeatMonitor consumersHeartbeat,
            IConfirmedResultOutbox outbox)
        {
            _outbox = outbox;
            _logger = logger;
            _serviceProvider = serviceProvider;
            _oracleClient = oracleClient;
            _producerHeartbeat = producerHeartbeat;
            _consumersHeartbeat = consumersHeartbeat;
            _pollingIntervalSeconds = configuration.GetValue<int>("OracleApi:PollingIntervalSeconds", 10);
            // Reparto asimétrico a propósito: AGENT tiene más workers porque el otro
            // aplicativo espera su resultado en línea; MACHINE no tiene esa presión de
            // latencia (nadie hace polling esperándolo con la misma urgencia).
            _agentMaxConcurrentWorkers = configuration.GetValue<int>("OracleApi:AgentMaxConcurrentWorkers", 4);
            _machineMaxConcurrentWorkers = configuration.GetValue<int>("OracleApi:MachineMaxConcurrentWorkers", 1);
            // Tamaño de tanda por poll — acota cuántas filas quedan reservadas (Envio = 2)
            // en RAM a la vez. No define el throughput real contra Oracle: eso lo hace el
            // rate limiter de OracleApiClient (partido en dos cupos, Agent/Machine), que
            // aplica sin importar cuánto traiga acá. Este valor evita reservar de más
            // entre un poll y el siguiente y además es el tope de filas en cola por canal
            // (el Producer solo reclama la capacidad libre, ver ProducerTask).
            _batchSize = configuration.GetValue<int>("OracleApi:BatchSize", 20);
            // Umbral para dar por huérfana una fila en Envio=2 y devolverla a 0. Con margen
            // sobre el peor caso razonable (una fila esperando turno en el Channel durante
            // un backlog grande), no sobre el caso típico (que resuelve en segundos). Nunca
            // reclama una fila en Envio=4 — ese estado significa que Oracle ya confirmó y
            // es intocable por diseño.
            _claimTimeoutMinutes = configuration.GetValue<int>("OracleApi:ClaimTimeoutMinutes", 20);
            // Edad a partir de la cual un resultado del outbox que no logra reaplicarse deja
            // de bloquear el reclamo de todo el lote: se marca en Envio=4 y se archiva en
            // expired/ (ver ReplayOutboxAsync). Holgado sobre las caídas observadas de SQL.
            _outboxExpireAfter = TimeSpan.FromMinutes(Math.Max(1, configuration.GetValue<int>("Outbox:ExpireAfterMinutes", 60)));

            _agentChannel = Channel.CreateUnbounded<CtiInteraccion>(new UnboundedChannelOptions { SingleWriter = true });
            _machineChannel = Channel.CreateUnbounded<CtiInteraccion>(new UnboundedChannelOptions { SingleWriter = true });

            _logger.LogInformation("CtiInteraccionProcessor iniciado — Polling: {Interval}s, AgentWorkers: {AgentWorkers}, MachineWorkers: {MachineWorkers}",
                _pollingIntervalSeconds, _agentMaxConcurrentWorkers, _machineMaxConcurrentWorkers);
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
                var agentWorkers = Enumerable.Range(1, _agentMaxConcurrentWorkers).Select(id => IniConsumerTask(id, _agentChannel, "Agent", stoppingToken));
                var machineWorkers = Enumerable.Range(1, _machineMaxConcurrentWorkers).Select(id => IniConsumerTask(id, _machineChannel, "Machine", stoppingToken));
                await Task.WhenAll(new[] { producer }.Concat(agentWorkers).Concat(machineWorkers));
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
                    await repo.InsertMachineOracle(ct);

                    // Mismo orden que en ProducerTask: replay del outbox antes de cualquier
                    // claim (en debug no hay reclamo por timeout, pero se mantiene el orden
                    // para que ambos modos se comporten igual).
                    await RunOutboxMaintenanceAsync();
                    await ReplayOutboxAsync(repo);

                    // AGENT primero: en modo debug (secuencial, sin canales) es el orden
                    // más simple para respetar la misma prioridad que en producción.
                    foreach (var origen in new[] { "AGENT", "MACHINE" })
                    {
                        var iniRecords = await repo.PopulateIniLLamada(_batchSize, origen, ct);
                        _logger.LogInformation("[DEBUG] {Count} registros IniLLamada ({Origen}) encontrados", iniRecords.Count, origen);
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
                                // Shutdown: no es un error del registro. Se propaga al catch
                                // del ciclo, que corta el loop sin reclamar ni persistir nada
                                // más (las filas del lote quedan en Envio=2 hasta el reclamo).
                                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                                {
                                    throw;
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogError(ex, "[DEBUG] Error procesando IniLLamada {LI}", record.LastInteractionId);
                                }
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
                    await repo.InsertMachineOracle(ct);

                    // Invariante de orden: el replay del outbox va SIEMPRE antes del reclamo
                    // por timeout. Una fila cuyo resultado Oracle ya confirmó pero que quedó
                    // en Envio=2 (SQL caído al persistir) solo existe como archivo en el
                    // outbox; si el reclamo corriera primero la devolvería a 0 y el Populate
                    // la reenviaría, creando una gestión duplicada. Si el replay lanza (SQL
                    // todavía no responde), la excepción sube al catch de este ciclo y el
                    // reclamo no se ejecuta — se reintenta todo en el próximo poll. La única
                    // excepción es un resultado vencido (más de Outbox:ExpireAfterMinutes)
                    // cuya fila ya quedó fuera de Envio=2: ese se archiva y el replay sigue,
                    // porque el reclamo ya no puede reenviarlo (ver ReplayOutboxAsync).
                    //
                    // El mantenimiento va justo antes y nunca lanza: deja en cuarentena los
                    // archivos ilegibles para que no queden invisibles para siempre.
                    await RunOutboxMaintenanceAsync();
                    await ReplayOutboxAsync(repo);

                    // Devuelve a Envio=0 las filas reservadas hace más de _claimTimeoutMinutes
                    // sin resolver — nunca toca Envio=4 (Oracle ya confirmó, es intocable).
                    // Va antes del Populate del ciclo para que lo recién liberado pueda
                    // reclamarse en el mismo ciclo, no en el siguiente.
                    var reclaimedIni = await repo.ReclaimOrphanedIniLLamada(_claimTimeoutMinutes, ct);
                    if (reclaimedIni > 0)
                        _logger.LogWarning("{Count} interacciones IniLLamada reclamadas por timeout (más de {TimeoutMin}min en Envio=2)", reclaimedIni, _claimTimeoutMinutes);

                    // Claim separado por Origen — cada uno alimenta su propio canal/pool,
                    // así un backlog de MACHINE nunca demora que un AGENT recién claimeado
                    // llegue a su worker. AGENT primero: si algo del lado de Oracle está
                    // degradado y hay que priorizar visibilidad, que sea AGENT quien entre
                    // primero al canal en este mismo ciclo.
                    //
                    // Solo se reclama la capacidad libre de cada canal (BatchSize menos lo que
                    // ya está en cola). Una fila que espera en el canal más que
                    // ClaimTimeoutMinutes sería reclamada por timeout y encolada otra vez →
                    // POST duplicado. Limitar la cola a BatchSize por pool mantiene la espera
                    // muy por debajo de ese timeout. Los canales Unbounded soportan Count.
                    var agentFree = _batchSize - _agentChannel.Reader.Count;
                    if (agentFree > 0)
                    {
                        var agentRecords = await repo.PopulateIniLLamada(agentFree, "AGENT", ct);
                        if (agentRecords.Any())
                        {
                            _logger.LogInformation("{Count} interacciones IniLLamada (AGENT) pendientes", agentRecords.Count);
                            foreach (var r in agentRecords)
                                await _agentChannel.Writer.WriteAsync(r, ct);
                        }
                    }
                    else
                    {
                        _logger.LogDebug("Canal AGENT lleno ({Pending} en cola, límite {BatchSize}) — no se reclaman filas en este ciclo", _agentChannel.Reader.Count, _batchSize);
                    }

                    var machineFree = _batchSize - _machineChannel.Reader.Count;
                    if (machineFree > 0)
                    {
                        var machineRecords = await repo.PopulateIniLLamada(machineFree, "MACHINE", ct);
                        if (machineRecords.Any())
                        {
                            _logger.LogInformation("{Count} interacciones IniLLamada (MACHINE) pendientes", machineRecords.Count);
                            foreach (var r in machineRecords)
                                await _machineChannel.Writer.WriteAsync(r, ct);
                        }
                    }
                    else
                    {
                        _logger.LogDebug("Canal MACHINE lleno ({Pending} en cola, límite {BatchSize}) — no se reclaman filas en este ciclo", _machineChannel.Reader.Count, _batchSize);
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
            _agentChannel.Writer.Complete();
            _machineChannel.Writer.Complete();
            _logger.LogInformation("Producer detenido");
        }

        // El await foreach de ReadAllAsync bloquea sin ejecutar el cuerpo mientras el
        // channel está vacío — con eso, ReportAlive()/ReportProgress() no se llaman
        // durante ventanas de baja actividad (de noche, fin de semana), y el Watchdog
        // termina reiniciando el proceso creyendo que está colgado cuando en realidad
        // no hay nada para procesar. Este intervalo hace que el worker "avise que sigue
        // vivo" aunque el channel esté vacío, sin necesidad de que llegue trabajo real.
        private static readonly TimeSpan IdleHeartbeatInterval = TimeSpan.FromSeconds(15);

        // Un solo método genérico para los dos pools (Agent/Machine) — reciben canales
        // distintos y distinta cantidad de instancias (ver ExecuteAsync), pero la lógica
        // de consumo es idéntica; poolLabel solo identifica el worker en los logs.
        private async Task IniConsumerTask(int workerId, Channel<CtiInteraccion> channel, string poolLabel, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                bool hasData;
                try
                {
                    var waitTask = channel.Reader.WaitToReadAsync(ct).AsTask();
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
                while (!ct.IsCancellationRequested && channel.Reader.TryRead(out var record))
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
                                _logger.LogDebug("Worker-{Pool} #{Id}: falla transitoria en {LI}, se reintentará en el próximo ciclo", poolLabel, workerId, record.LastInteractionId);
                            }
                            else
                            {
                                // Success no loguea nada acá — es el caso esperado, la
                                // mayoría de los ítems, y loguearlo uno por uno satura el
                                // archivo sin aportar señal. PermanentFailure sí interesa
                                // ver (poco volumen, y OracleApiClient ya lo logueó con
                                // más detalle — esto solo correlaciona qué worker lo tomó).
                                if (outcome == ApiOutcome.PermanentFailure)
                                    _logger.LogInformation("Worker-{Pool} #{Id}: falla permanente en {LI}", poolLabel, workerId, record.LastInteractionId);
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
                            _logger.LogError(ex, "Worker-{Pool} #{Id} falló en {LI}", poolLabel, workerId, record.LastInteractionId);
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
            // Origen=MACHINE: dFin_c = dInicio_c, un solo evento que el agente nunca toca.
            // Origen=AGENT: dFin_c real (FechaFinLLamada) — PopulateIniLLamada ya garantiza
            // que este dato existe antes de reclamar la fila.
            var esAgent = record.Origen == "AGENT";
            var request = new OracleIniLlamadaRequest
            {
                tANI_c = record.Celular,
                tProveedor_c = record.Proveedor,
                dInicio_c = FormatFecha(record.FechaIniLLamada),
                dFin_c = esAgent ? FormatFecha(record.FechaFinLLamada) : FormatFecha(record.FechaIniLLamada),
                tUCID_c = record.LastInteractionId,
                chTipo_c = record.Tipo,
                chOpty_Id_c = record.IdOportunidad,
                tUsuarioNumDoc_c = record.AgenteId,
                chOptyTipifResultado_c = record.Resultado,
                chOptyTipifSubResultado_c = record.Motivo
            };

            var (outcome, response, rawBody) = await _oracleClient.IniciarGestionAsync(request, record.Origen!, ct);

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
            // Solo para AGENT: recuperar la tipificación de la respuesta de Oracle en vez
            // de mandarla nosotros. Para MACHINE quedan en null y UpdateIniLLamada no toca
            // Resultado/Motivo (ya vienen correctos desde antes).
            string? resultado = esAgent ? response?.chOptyTipifResultado_c : null;
            string? motivo = esAgent ? response?.chOptyTipifSubResultado_c : null;

            // Mismo NullValueHandling.Ignore que usa OracleApiClient para armar el body
            // real — así JsonIni queda fiel a lo que de verdad viajó a Oracle, sin mostrar
            // "chOptyTipifResultado_c": null cuando en realidad esa propiedad ni siquiera se
            // envió. Se calculan una sola vez: el outbox y el UPDATE deben guardar
            // exactamente lo mismo.
            var jsonIni = JsonConvert.SerializeObject(request, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            var respuestaIni = rawBody ?? JsonConvert.SerializeObject(response);

            // Outbox ANTES del UPDATE: si SQL está caído, este archivo es la única prueba
            // local de que Oracle ya resolvió esta interacción. El replay del Producer lo
            // reaplica cuando SQL vuelve, siempre antes del reclamo por timeout, así la
            // fila no se reenvía (Oracle no tiene GET por tUCID_c para chequear antes).
            var pending = new PendingIniResult
            {
                JsonIni = jsonIni,
                RespuestaIni = respuestaIni,
                EnvioIniLLamada = envio,
                LastInteractionId = record.LastInteractionId!,
                IdOracle = idOracle,
                UrlOracle = urlOracle,
                Resultado = resultado,
                Motivo = motivo,
                CreatedAtUtc = DateTime.UtcNow
            };

            bool savedToOutbox = false;
            try
            {
                await _outbox.SaveAsync(pending);
                savedToOutbox = true;
            }
            catch (Exception ex)
            {
                // Nunca bloquea el flujo: sin outbox se sigue con el comportamiento previo
                // (UPDATE directo y, si falla, Envio=4 o log para corrección manual).
                _logger.LogError(ex, "No se pudo escribir el outbox local para {LI} — se continúa sin respaldo local, con el comportamiento previo", record.LastInteractionId);
            }

            try
            {
                await repo.UpdateIniLLamada(
                    jsonIni,
                    respuestaIni,
                    envio,
                    record.LastInteractionId!,
                    idOracle,
                    urlOracle,
                    resultado,
                    motivo);

                if (savedToOutbox)
                {
                    try
                    {
                        await _outbox.RemoveAsync(record.LastInteractionId!);
                    }
                    catch (Exception ex)
                    {
                        // Inofensivo: el replay vuelve a aplicar el mismo UPDATE (idempotente)
                        // y recién ahí borra el archivo.
                        _logger.LogWarning(ex, "No se pudo borrar el archivo de outbox de {LI} tras persistir en SQL — el replay lo reaplicará (UPDATE idempotente) y lo borrará", record.LastInteractionId);
                    }
                }
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
                //
                // Con el outbox escrito, el caso deja de requerir intervención: el replay
                // del Producer reaplica el UPDATE completo (que pisa Envio=4 con el valor
                // real) apenas SQL vuelve, y lo hace antes del reclamo por timeout.
                if (outcome == ApiOutcome.Success)
                {
                    var marked = await repo.MarkIniLLamadaConfirmedUnpersisted(record.LastInteractionId!);
                    if (savedToOutbox)
                        _logger.LogError(ex, "No se pudo persistir el éxito de {LI} — Oracle ya procesó esta interacción (IdOracle={IdOracle}, UrlOracle={UrlOracle}). Resultado resguardado en el outbox local (Envio=4 marcado: {Marked}): se reaplicará solo cuando SQL vuelva, antes de cualquier reclamo por timeout.", record.LastInteractionId, idOracle, urlOracle, marked);
                    else if (marked)
                        _logger.LogError(ex, "No se pudo persistir el éxito de {LI} ni escribir el outbox — Oracle ya procesó esta interacción (IdOracle={IdOracle}, UrlOracle={UrlOracle}). Marcada en Envio=4, no se reintentará sola: requiere corrección manual (UPDATE Envio=1, IdOracle, UrlOracle) en GSS_OraclePacifico.", record.LastInteractionId, idOracle, urlOracle);
                    else
                        _logger.LogError(ex, "No se pudo persistir el éxito de {LI} ni escribir el outbox ni marcarla en Envio=4 — Oracle ya procesó esta interacción (IdOracle={IdOracle}, UrlOracle={UrlOracle}). La fila queda en Envio=2: el reclamo por timeout la va a reenviar si no se corrige a mano antes.", record.LastInteractionId, idOracle, urlOracle);
                }
                else if (savedToOutbox)
                {
                    _logger.LogError(ex, "No se pudo persistir el resultado (PermanentFailure) de {LI} — resguardado en el outbox local: se reaplicará solo cuando SQL vuelva, antes de cualquier reclamo por timeout.", record.LastInteractionId);
                }
                else
                {
                    _logger.LogError(ex, "No se pudo persistir el resultado (PermanentFailure) de {LI} ni escribir el outbox — la fila queda en Envio=2, elegible para reclamo por timeout.", record.LastInteractionId);
                }
            }

            return outcome;
        }

        // Limpieza de disco del outbox (sin base): .tmp abandonados, cuarentena de
        // ilegibles y retención de expired/ y quarantine/. Atrapa todo: una falla acá no
        // debe impedir el replay ni el reclamo, que son los que protegen contra duplicados.
        // El store devuelve qué hizo con cada archivo y el log se emite acá para poder
        // empujar el LastInteractionId al ContactId del LogContext (ver consumer loop).
        private async Task RunOutboxMaintenanceAsync()
        {
            OutboxMaintenanceResult result;
            try
            {
                result = await _outbox.RunMaintenanceAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mantenimiento del outbox falló — se continúa con el replay de este ciclo");
                return;
            }

            var archiveDeleted = 0;
            foreach (var entry in result.Entries)
            {
                if (entry.Action == OutboxMaintenanceAction.ArchiveDeleted)
                {
                    archiveDeleted++;
                    continue;
                }

                // Falla de la carpeta entera (enumerar), no de una interacción puntual.
                if (string.IsNullOrEmpty(entry.LastInteractionId))
                {
                    _logger.LogWarning(entry.Error, "Outbox: no se pudo {Operacion} ({OutboxPath}) — se reintenta en el próximo ciclo", entry.Detail, entry.FileName);
                    continue;
                }

                using (LogContext.PushProperty("ContactId", entry.LastInteractionId))
                {
                    switch (entry.Action)
                    {
                        case OutboxMaintenanceAction.TempDeleted:
                            _logger.LogWarning("Outbox: se borró un .tmp abandonado de {LI} ({OutboxFile}) — escritura que nunca terminó; si la fila sigue en Envio=2 el reclamo por timeout la reenviará", entry.LastInteractionId, entry.FileName);
                            break;
                        case OutboxMaintenanceAction.Quarantined:
                            _logger.LogError(entry.Error, "Outbox: archivo ilegible de {LI} movido a cuarentena ({OutboxFile} → {QuarantinePath}) — no se reaplica solo: revisar a mano y, si Oracle ya creó la gestión, corregir la fila en GSS_OraclePacifico antes de que el reclamo por timeout la reenvíe", entry.LastInteractionId, entry.FileName, entry.Detail);
                            break;
                        default:
                            _logger.LogWarning(entry.Error, "Outbox: no se pudo {Operacion} para {LI} ({OutboxFile}) — se reintenta en el próximo ciclo", entry.Detail, entry.LastInteractionId, entry.FileName);
                            break;
                    }
                }
            }

            if (archiveDeleted > 0)
                _logger.LogInformation("Outbox: {Count} archivos de expired/quarantine purgados por retención", archiveDeleted);
        }

        // Reaplica en SQL los resultados que quedaron en el outbox local porque la
        // persistencia original falló (o porque el borrado posterior falló). El UPDATE
        // es idempotente y fija EnvioIniLLamada sin condición, así que también corrige
        // una fila que había quedado en Envio=4. Si UpdateIniLLamada lanza, la excepción
        // sube a propósito: corta el replay de este ciclo y, con eso, el reclamo por
        // timeout que va después (ver ProducerTask).
        //
        // Única salida sin lanzar: un resultado más viejo que Outbox:ExpireAfterMinutes
        // cuya falla no es de conectividad. Sin esa salida, un solo archivo que nunca
        // logra aplicarse (dato inválido, por ejemplo) bloquearía el reclamo de todas
        // las demás filas para siempre.
        private async Task ReplayOutboxAsync(IInteraccionRepository repo)
        {
            var pending = await _outbox.ListPendingAsync();
            if (pending.Count == 0)
                return;

            var replayed = 0;
            var expired = 0;
            foreach (var item in pending)
            {
                // Mismo motivo que en el consumer loop: la columna ContactId de
                // GSS_LogPacifico guarda el LastInteractionId, que es único por llamada.
                using (LogContext.PushProperty("ContactId", item.LastInteractionId))
                {
                    try
                    {
                        var updated = await repo.UpdateIniLLamada(
                            item.JsonIni,
                            item.RespuestaIni,
                            item.EnvioIniLLamada,
                            item.LastInteractionId,
                            item.IdOracle,
                            item.UrlOracle,
                            item.Resultado,
                            item.Motivo);

                        // false = 0 filas: la fila ya no existe en GSS_OraclePacifico. Reintentar
                        // no cambiaría nada, así que el archivo se borra igual (queda el log).
                        if (!updated)
                            _logger.LogWarning("Replay de outbox para {LI}: el UPDATE no afectó ninguna fila (la fila ya no existe) — se descarta el archivo (IdOracle={IdOracle}, UrlOracle={UrlOracle})", item.LastInteractionId, item.IdOracle, item.UrlOracle);
                        else
                            replayed++;

                        await _outbox.RemoveAsync(item.LastInteractionId);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        var age = DateTime.UtcNow - item.CreatedAtUtc;
                        if (age < _outboxExpireAfter || IsSqlConnectivityFailure(ex))
                        {
                            // Reciente, o SQL inalcanzable: el archivo se queda y el replay se
                            // corta. Con SQL caído no se expira nada aunque sea viejo — una
                            // caída larga no debe vaciar el outbox justo cuando más se necesita.
                            _logger.LogWarning(ex, "Replay de outbox para {LI} falló (antigüedad {AgeMin:F0} min, IdOracle={IdOracle}, UrlOracle={UrlOracle}) — se conserva el archivo y se omite el reclamo por timeout en este ciclo", item.LastInteractionId, age.TotalMinutes, item.IdOracle, item.UrlOracle);
                            throw;
                        }

                        // Vencido con la base respondiendo: llevar la fila a Envio=4 (estado que
                        // el reclamo nunca toca) antes de sacar el archivo del replay. Archivar
                        // con la fila todavía en Envio=2 dejaría que el reclamo la reenvíe,
                        // creando la gestión duplicada que el outbox existe para evitar.
                        bool marked;
                        try
                        {
                            marked = await repo.MarkIniLLamadaConfirmedUnpersisted(item.LastInteractionId);
                        }
                        catch (Exception markEx) when (markEx is not OperationCanceledException)
                        {
                            _logger.LogWarning(markEx, "Replay de outbox para {LI}: no se pudo marcar en Envio=4 (IdOracle={IdOracle}, UrlOracle={UrlOracle}) — se conserva el archivo y se omite el reclamo por timeout en este ciclo", item.LastInteractionId, item.IdOracle, item.UrlOracle);
                            throw;
                        }

                        // MarkIniLLamadaConfirmedUnpersisted atrapa sus propios errores y devuelve
                        // false, así que false es ambiguo: "la fila ya no está en Envio=2" o "la
                        // base falló". Solo se acepta false como seguro si el UPDATE original
                        // obtuvo una respuesta del servidor (SqlException no de conectividad):
                        // esa es la evidencia de que la base estaba accesible y el false es el
                        // del WHERE EnvioIniLLamada = 2. Cualquier otra falla (sin SqlException)
                        // no prueba nada, y se conserva el archivo. Riesgo residual aceptado: que
                        // la base caiga justo entre el UPDATE y el Mark.
                        if (!marked && !ContainsSqlException(ex))
                        {
                            _logger.LogWarning(ex, "Replay de outbox para {LI} vencido pero sin evidencia de que la fila haya salido de Envio=2 (IdOracle={IdOracle}, UrlOracle={UrlOracle}) — se conserva el archivo y se omite el reclamo por timeout en este ciclo", item.LastInteractionId, item.IdOracle, item.UrlOracle);
                            throw;
                        }

                        try
                        {
                            var expiredPath = await _outbox.MoveToExpiredAsync(item.LastInteractionId);
                            expired++;
                            _logger.LogError(ex, "Replay de outbox para {LI} vencido (antigüedad {AgeMin:F0} min): no se pudo reaplicar y se movió a {ExpiredPath} (Envio=4 marcado: {Marked}). Oracle ya resolvió esta interacción (IdOracle={IdOracle}, UrlOracle={UrlOracle}, Envio esperado={Envio}). Requiere corrección manual en GSS_OraclePacifico con los datos del archivo en outbox\\expired: UPDATE EnvioIniLLamada, IdOracle, UrlOracle, Resultado y Motivo de esa LastInteractionId.",
                                item.LastInteractionId, age.TotalMinutes, expiredPath, marked, item.IdOracle, item.UrlOracle, item.EnvioIniLLamada);
                        }
                        catch (Exception moveEx) when (moveEx is IOException or UnauthorizedAccessException)
                        {
                            // Se sigue igual: la fila ya está fuera de Envio=2, así que el reclamo
                            // no puede reenviarla. El próximo ciclo reintenta el movimiento.
                            _logger.LogError(moveEx, "Replay de outbox para {LI} vencido: la fila ya no está en Envio=2 (Envio=4 marcado: {Marked}) pero no se pudo mover el archivo a expired — se reintenta en el próximo ciclo (IdOracle={IdOracle}, UrlOracle={UrlOracle}, Envio esperado={Envio})", item.LastInteractionId, marked, item.IdOracle, item.UrlOracle, item.EnvioIniLLamada);
                        }
                    }
                }
            }

            _logger.LogWarning("Replay de outbox: {Replayed} de {Total} resultados pendientes reaplicados en SQL, {Expired} movidos a expired", replayed, pending.Count, expired);
        }

        // Errores de SQL Server/red que indican que la base no se alcanzó (o se perdió la
        // conexión), no que rechazó el dato: timeout (-2), red/instancia (53, 40, 2, 64,
        // 121, 233, 1231, 11001, 10053, 10054, 10060) y base no disponible (4060). Class
        // >= 20 son errores fatales de conexión. Se recorre toda la cadena de
        // InnerException porque Polly envuelve la SqlException original en
        // BrokenCircuitException cuando el circuito está abierto; un circuito abierto
        // cuenta como conectividad aunque no traiga causa, por la misma razón.
        private static readonly HashSet<int> SqlConnectivityErrorNumbers = new()
        {
            -2, 2, 40, 53, 64, 121, 233, 1231, 4060, 10053, 10054, 10060, 11001
        };

        private static bool IsSqlConnectivityFailure(Exception ex)
        {
            for (var current = ex; current is not null; current = current.InnerException)
            {
                switch (current)
                {
                    case BrokenCircuitException:
                    case TimeoutException:
                    case SocketException:
                        return true;
                    case SqlException sql:
                        if (sql.Class >= 20)
                            return true;
                        foreach (SqlError error in sql.Errors)
                        {
                            if (SqlConnectivityErrorNumbers.Contains(error.Number))
                                return true;
                        }
                        if (SqlConnectivityErrorNumbers.Contains(sql.Number))
                            return true;
                        break;
                }
            }
            return false;
        }

        private static bool ContainsSqlException(Exception ex)
        {
            for (var current = ex; current is not null; current = current.InnerException)
            {
                if (current is SqlException)
                    return true;
            }
            return false;
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
