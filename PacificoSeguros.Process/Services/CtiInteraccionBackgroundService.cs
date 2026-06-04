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

        private readonly Channel<CtiInteraccion> _iniChannel;
        private readonly Channel<CtiInteraccion> _finChannel;

        private readonly int _pollingIntervalSeconds;
        private readonly int _maxConcurrentWorkers;

        public CtiInteraccionBackgroundService(
            ILogger<CtiInteraccionBackgroundService> logger,
            IServiceProvider serviceProvider,
            IConfiguration configuration,
            IOracleApiClient oracleClient)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _configuration = configuration;
            _oracleClient = oracleClient;
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
                        using (LogContext.PushProperty("Celular", record.Celular))
                        using (LogContext.PushProperty("ContactId", record.ContactId))
                        {
                            _logger.LogInformation("[DEBUG] Procesando IniLLamada: {LI}", record.LastInteractionId);
                            await ProcessIniLlamada(repo, record);
                        }
                    }

                    var finRecords = await repo.PopulateFinLLamada();
                    _logger.LogInformation("[DEBUG] {Count} registros FinLLamada encontrados", finRecords.Count);
                    foreach (var record in finRecords)
                    {
                        using (LogContext.PushProperty("Celular", record.Celular))
                        using (LogContext.PushProperty("ContactId", record.ContactId))
                        {
                            _logger.LogInformation("[DEBUG] Procesando FinLLamada: {LI}", record.LastInteractionId);
                            await ProcessFinLlamada(repo, record);
                        }
                    }

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

        private async Task IniConsumerTask(int workerId, CancellationToken ct)
        {
            await foreach (var record in _iniChannel.Reader.ReadAllAsync(ct))
            {
                using (LogContext.PushProperty("Celular", record.Celular))
                using (LogContext.PushProperty("ContactId", record.ContactId))
                {
                    try
                    {
                        using var scope = _serviceProvider.CreateScope();
                        var repo = scope.ServiceProvider.GetRequiredService<IInteraccionRepository>();
                        _logger.LogInformation("Worker-Ini #{Id} procesando {LI}", workerId, record.LastInteractionId);
                        await ProcessIniLlamada(repo, record);
                        _logger.LogInformation("Worker-Ini #{Id} completó {LI}", workerId, record.LastInteractionId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Worker-Ini #{Id} falló en {LI}", workerId, record.LastInteractionId);
                    }
                }
            }
        }

        private async Task FinConsumerTask(int workerId, CancellationToken ct)
        {
            await foreach (var record in _finChannel.Reader.ReadAllAsync(ct))
            {
                using (LogContext.PushProperty("Celular", record.Celular))
                using (LogContext.PushProperty("ContactId", record.ContactId))
                {
                    try
                    {
                        using var scope = _serviceProvider.CreateScope();
                        var repo = scope.ServiceProvider.GetRequiredService<IInteraccionRepository>();
                        _logger.LogInformation("Worker-Fin #{Id} procesando {LI}", workerId, record.LastInteractionId);
                        await ProcessFinLlamada(repo, record);
                        _logger.LogInformation("Worker-Fin #{Id} completó {LI}", workerId, record.LastInteractionId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Worker-Fin #{Id} falló en {LI}", workerId, record.LastInteractionId);
                    }
                }
            }
        }

        private async Task ProcessIniLlamada(IInteraccionRepository repo, CtiInteraccion record)
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

            var response = await _oracleClient.IniciarGestionAsync(request);

            int envio = response?.Id > 0 ? 1 : 3;
            long idOracle = response?.Id ?? 0;
            string urlOracle = response?.tURL_c??"";

            await repo.UpdateIniLLamada(
                JsonConvert.SerializeObject(request),
                JsonConvert.SerializeObject(response),
                envio,
                record.LastInteractionId!,
                idOracle,
                urlOracle);
        }

        private async Task ProcessFinLlamada(IInteraccionRepository repo, CtiInteraccion record)
        {
            var request = new OracleFinLlamadaRequest { dFin_c = FormatFecha(record.FechaFinLLamada) };
            string? responseBody = await _oracleClient.FinalizarGestionAsync( request,record.IdOracle ?? 0L);
            await repo.UpdateFinLLamada(
                JsonConvert.SerializeObject(request),
                responseBody ?? string.Empty,
                responseBody != null ? 1 : 3,
                record.LastInteractionId!);
        }

        private static string FormatFecha(DateTime? fecha) =>
            fecha.HasValue ? fecha.Value.ToString("yyyy-MM-ddTHH:mm:ss.fff-05:00") : string.Empty;
    }
}
