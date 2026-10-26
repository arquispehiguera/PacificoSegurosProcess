using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.RateLimiting;

namespace PacificoSeguros.Infraestructure.Services
{
    public class OracleApiClient : IOracleApiClient, IDisposable
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<OracleApiClient> _logger;

        private string? _accessToken;
        private DateTime _tokenExpiresAt = DateTime.MinValue;
        private readonly SemaphoreSlim _tokenLock = new(1, 1);
        private readonly TimeSpan _lockTimeout;

        // Cupo de Oracle definido por el cliente: 100 requests/min en total, ahora repartido
        // en dos baldes independientes en vez de uno compartido — así una ráfaga de MACHINE
        // nunca puede consumir el cupo que necesita AGENT (el otro aplicativo poll-ea la
        // tabla esperando Resultado/Motivo en línea, y ese camino no puede quedar atrás de
        // MACHINE bajo congestión). AgentRateLimitPerMinute + MachineRateLimitPerMinute no
        // tienen por qué sumar 100 — son cupos independientes, no una partición estricta.
        // SlidingWindow en vez de FixedWindow: un contador que resetea en un instante fijo
        // permite ráfagas de hasta 2x el límite pegadas al borde de la ventana — con un
        // cupo tan ajustado eso alcanza para volver a gatillar el 429. Con cola (en vez de
        // rechazo inmediato) el worker que no tiene turno espera en vez de fallar.
        private readonly RateLimiter _agentRateLimiter;
        private readonly RateLimiter _machineRateLimiter;

        public OracleApiClient(
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration,
            ILogger<OracleApiClient> logger)
        {
            _httpClientFactory = httpClientFactory;
            _configuration = configuration;
            _logger = logger;
            _lockTimeout = TimeSpan.FromSeconds(configuration.GetValue<int>("OracleApi:TokenLockTimeoutSeconds", 30));
            _agentRateLimiter = new SlidingWindowRateLimiter(new SlidingWindowRateLimiterOptions
            {
                PermitLimit = configuration.GetValue<int>("OracleApi:AgentRateLimitPerMinute", 70),
                Window = TimeSpan.FromMinutes(1),
                SegmentsPerWindow = 6,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                QueueLimit = 1000
            });
            _machineRateLimiter = new SlidingWindowRateLimiter(new SlidingWindowRateLimiterOptions
            {
                PermitLimit = configuration.GetValue<int>("OracleApi:MachineRateLimitPerMinute", 30),
                Window = TimeSpan.FromMinutes(1),
                SegmentsPerWindow = 6,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                QueueLimit = 1000
            });
        }

        public async Task<(ApiOutcome Outcome, OracleInteraccionResponse? Response, string? RawBody)> IniciarGestionAsync(OracleIniLlamadaRequest request, string origen, CancellationToken ct)
        {
            var url = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:IniLlamadaPath"]}";
            var subscriptionKey = _configuration["OracleApi:SubscriptionKey"]!;
            // NullValueHandling.Ignore: chOptyTipifResultado_c/chOptyTipifSubResultado_c van en null
            // para Origen=AGENT (recién se recuperan de la respuesta de este mismo llamado) —
            // sin esto, Newtonsoft los manda igual como "prop": null en vez de omitirlos del JSON.
            var jsonBody = JsonConvert.SerializeObject(request, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

            try
            {
                var (outcome, body) = await SendAsync(origen, token =>
                    BuildRequest(url, token, subscriptionKey, jsonBody), ct);

                if (outcome != ApiOutcome.Success)
                {
                    if (outcome == ApiOutcome.TransientFailure)
                        _logger.LogDebug("IniciarGestion transitorio ({Outcome}), se reintentará en el próximo ciclo: {Body}", outcome, body);
                    else
                        _logger.LogWarning("IniciarGestion no exitoso ({Outcome}): {Body}", outcome, body);
                    // TransientFailure libera la fila (nada que guardar). PermanentFailure
                    // devuelve el body (con el status HTTP o la marca de timeout) para que
                    // quede en RespuestaIni y se pueda revisar a mano.
                    return (outcome, null, outcome == ApiOutcome.TransientFailure ? null : body);
                }
                // Se devuelve el body crudo tal cual lo manda Oracle (no solo el objeto
                // deserializado) para que quede completo en RespuestaIni — el objeto tipado
                // solo trae los campos que el código necesita (Id, tURL_c, tipificación),
                // pero Oracle devuelve muchos más y sirven para auditoría/soporte.
                OracleInteraccionResponse? response;
                try
                {
                    response = JsonConvert.DeserializeObject<OracleInteraccionResponse>(body);
                }
                catch (JsonException ex)
                {
                    // Oracle respondió 2xx: la gestión puede existir aunque el body no se
                    // pueda leer. Se guarda el body crudo como definitivo, nunca se reenvía.
                    _logger.LogError(ex, "IniciarGestion: Oracle respondió 2xx pero el body no se pudo leer — la gestión puede existir en Oracle, requiere revisión manual: {Body}", body);
                    return (ApiOutcome.PermanentFailure, null, body);
                }
                return (ApiOutcome.Success, response, body);
            }
            // Un shutdown no es una falla de la fila: se propaga para que no se persista
            // nada (la fila queda en Envio=2 y el reclamo por timeout la libera después).
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "IniciarGestion: fallo no clasificado, se marca como definitivo para revisión manual");
                return (ApiOutcome.PermanentFailure, null, null);
            }
        }

        // Handles Polly retries + single token refresh on 401
        private async Task<(ApiOutcome Outcome, string Body)> SendAsync(string origen, Func<string, HttpRequestMessage> buildRequest, CancellationToken ct)
        {
            var rateLimiter = origen == "MACHINE" ? _machineRateLimiter : _agentRateLimiter;
            bool tokenRefreshed = false;
            while (true)
            {
                var (tokenOutcome, token) = await GetTokenAsync(ct);
                if (tokenOutcome != ApiOutcome.Success)
                    return (tokenOutcome, string.Empty);

                try
                {
                    // El permiso del rate limiter se adquiere dentro del delegado que Polly
                    // reintenta, no una sola vez afuera. Si se adquiriera una sola vez antes
                    // de HttpRetry, un solo permiso alcanzaba para autorizar hasta 3 llamadas
                    // físicas reales a Oracle (el intento inicial + los 2 reintentos de
                    // HttpRetry ante un 429), dejando pasar de largo el cupo real durante
                    // exactamente la tormenta que este límite existe para frenar. Con el
                    // permiso adentro, cada intento físico — inicial o reintento — paga su
                    // propio lugar en el balde de Agent o Machine (según origen). `ct` se
                    // propaga hasta el SendAsync real y el ReadAsStringAsync, no solo hasta
                    // el AcquireAsync, para que un shutdown grácil corte una request colgada
                    // en vez de esperar el timeout de HttpClient.
                    //
                    // IniLlamadaRetry (no HttpRetry): el POST no es idempotente, así que un
                    // timeout de HttpClient no se reintenta (ver ResiliencePolicies).
                    var (ok, status, body) = await ResiliencePolicies.IniLlamadaRetry.ExecuteAsync<(bool, HttpStatusCode, string)>(async innerCt =>
                    {
                        using var lease = await rateLimiter.AcquireAsync(1, innerCt);
                        if (!lease.IsAcquired)
                            throw new HttpRequestException("rate limiter: cola llena, se reintentará");

                        using var req = buildRequest(token);
                        var r = await _httpClientFactory.CreateClient().SendAsync(req, innerCt);
                        var b = await r.Content.ReadAsStringAsync(innerCt);
                        if (IsRetryableForIniLlamada(r.StatusCode))
                            throw new HttpRequestException($"[{(int)r.StatusCode}] {b}");
                        return (r.IsSuccessStatusCode, r.StatusCode, b);
                    }, ct);

                    if (status == HttpStatusCode.Unauthorized)
                    {
                        if (!tokenRefreshed)
                        {
                            tokenRefreshed = true;
                            await InvalidateTokenAsync();
                            continue;
                        }
                        // 401 aun con token recién renovado: problema de credenciales o
                        // configuración que afecta a todas las filas, no a esta en particular.
                        // Oracle rechazó la request sin procesarla, así que liberar la fila es
                        // seguro; marcarla en Envio=3 perdería todas las filas del período.
                        _logger.LogError("SendAsync: Oracle respondió 401 incluso después de renovar el token — problema de credenciales/configuración que afecta a todas las filas; se libera la fila y se reintentará en el próximo ciclo: {Body}", body);
                        return (ApiOutcome.TransientFailure, string.Empty);
                    }

                    // El status HTTP va como prefijo para que quede visible en RespuestaIni
                    // (incluye 500/502/504, que no se reintentan: resultado desconocido).
                    return ok ? (ApiOutcome.Success, body) : (ApiOutcome.PermanentFailure, $"[{(int)status}] {body}");
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (OperationCanceledException ex)
                {
                    // Con `ct` sin cancelar, esto es el timeout de HttpClient: la request pudo
                    // haber llegado y Oracle pudo haber creado la gestión. No se libera la fila
                    // (TransientFailure la devolvería a Envio=0 y se reenviaría duplicada): se
                    // marca como definitiva para revisión manual.
                    _logger.LogError(ex, "SendAsync: timeout de HttpClient esperando a Oracle — resultado desconocido (la gestión puede existir), no se reintenta; requiere revisión manual");
                    return (ApiOutcome.PermanentFailure, "[timeout] Sin respuesta de Oracle dentro del timeout de HttpClient — resultado desconocido, la gestión puede existir en Oracle: revisar manualmente antes de reenviar");
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "SendAsync: fallo transitorio tras agotar reintentos — se reintentará en el próximo ciclo");
                    return (ApiOutcome.TransientFailure, string.Empty);
                }
            }
        }

        public void Dispose()
        {
            _agentRateLimiter.Dispose();
            _machineRateLimiter.Dispose();
        }

        private async Task<(ApiOutcome Outcome, string Token)> GetTokenAsync(CancellationToken ct)
        {
            if (!await _tokenLock.WaitAsync(_lockTimeout, ct))
            {
                _logger.LogDebug("GetToken: no se pudo obtener el lock a tiempo — otro worker lo tiene retenido, se reintentará en el próximo ciclo");
                return (ApiOutcome.TransientFailure, string.Empty);
            }
            try
            {
                if (!string.IsNullOrEmpty(_accessToken) && DateTime.UtcNow < _tokenExpiresAt)
                    return (ApiOutcome.Success, _accessToken);

                _logger.LogInformation("Obteniendo token Oracle...");

                var body = JsonConvert.SerializeObject(new { resource = _configuration["OracleApi:Resource"] });
                var tokenUrl = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:TokenPath"]}";

                bool isSuccess;
                string responseBody;

                try
                {
                    (isSuccess, responseBody) = await ResiliencePolicies.TokenRequestPolicy.ExecuteAsync<(bool, string)>(async innerCt =>
                    {
                        using var req = new HttpRequestMessage(HttpMethod.Post, tokenUrl);
                        req.Headers.Add("Ocp-Apim-Subscription-Key", _configuration["OracleApi:TokenSubscriptionKey"]);
                        req.Headers.Add("clientcredential", _configuration["OracleApi:ClientCredential"]);
                        req.Content = new StringContent(body, Encoding.UTF8, "application/json");

                        var r = await _httpClientFactory.CreateClient().SendAsync(req, innerCt);
                        var b = await r.Content.ReadAsStringAsync(innerCt);
                        if (IsRetryableForToken(r.StatusCode))
                            throw new HttpRequestException($"[{(int)r.StatusCode}] {b}");
                        return (r.IsSuccessStatusCode, b);
                    }, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "GetToken: fallo transitorio (rate limit / circuito abierto / red) — se reintentará en el próximo ciclo");
                    return (ApiOutcome.TransientFailure, string.Empty);
                }

                // Una falla del token no es de la fila sino del sistema (ej. ClientCredential
                // rotada): devolver PermanentFailure marcaría en Envio=3 cada fila del período.
                // TransientFailure libera la fila y se reintenta en el próximo ciclo; el POST
                // nunca salió, así que no hay riesgo de duplicado.
                if (!isSuccess)
                {
                    _logger.LogError("Error al obtener token Oracle — problema de credenciales/configuración que afecta a todas las filas, se reintentará en el próximo ciclo: {Body}", responseBody);
                    return (ApiOutcome.TransientFailure, string.Empty);
                }

                OracleTokenResponse? tokenResponse;
                try
                {
                    tokenResponse = JsonConvert.DeserializeObject<OracleTokenResponse>(responseBody);
                }
                catch (JsonException)
                {
                    tokenResponse = null;
                }
                if (tokenResponse is null || string.IsNullOrEmpty(tokenResponse.access_token))
                {
                    _logger.LogError("Respuesta de token vacía o inválida — problema de configuración que afecta a todas las filas, se reintentará en el próximo ciclo: {Body}", responseBody);
                    return (ApiOutcome.TransientFailure, string.Empty);
                }

                _accessToken = tokenResponse.access_token;

                if (long.TryParse(tokenResponse.expires_on, out var expiresOnUnix))
                {
                    _tokenExpiresAt = DateTimeOffset.FromUnixTimeSeconds(expiresOnUnix).UtcDateTime.AddMinutes(-5);
                    _logger.LogInformation("Token Oracle obtenido — expira en {ExpiresAt} UTC", _tokenExpiresAt);
                }
                else
                {
                    _tokenExpiresAt = DateTime.UtcNow.AddMinutes(50);
                    _logger.LogWarning("No se pudo parsear expires_on — usando expiración de 50 min por defecto");
                }

                return (ApiOutcome.Success, _accessToken);
            }
            finally
            {
                _tokenLock.Release();
            }
        }

        private async Task InvalidateTokenAsync()
        {
            if (!await _tokenLock.WaitAsync(_lockTimeout))
                return; // otro worker ya está refrescando el token — no pisar su trabajo, no lanzar acá
            try
            {
                _accessToken = null;
                _tokenExpiresAt = DateTime.MinValue;
            }
            finally
            {
                _tokenLock.Release();
            }
        }

        // El POST de IniLlamada no es idempotente: ante un 500/502/504 Oracle pudo haber
        // creado la gestión antes de que el gateway respondiera con error, y reintentar
        // crearía un duplicado. Solo 429 y 503 indican que la request fue rechazada sin
        // procesarse.
        private static bool IsRetryableForIniLlamada(HttpStatusCode status) =>
            status == HttpStatusCode.TooManyRequests ||
            status == HttpStatusCode.ServiceUnavailable;

        // La request de token sí es idempotente (pedir otro token no tiene efectos
        // colaterales), así que ahí se mantienen todos los 5xx transitorios.
        private static bool IsRetryableForToken(HttpStatusCode status) =>
            status == HttpStatusCode.TooManyRequests ||
            status == HttpStatusCode.InternalServerError ||
            status == HttpStatusCode.BadGateway ||
            status == HttpStatusCode.ServiceUnavailable ||
            status == HttpStatusCode.GatewayTimeout;

        private static HttpRequestMessage BuildRequest(string url, string token, string subscriptionKey, string jsonBody)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.Add("Ocp-Apim-Subscription-Key", subscriptionKey);
            request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            return request;
        }
    }
}
