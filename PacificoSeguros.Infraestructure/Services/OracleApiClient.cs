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

        // Cupo de Oracle definido por el cliente: 100 requests/min, compartido entre
        // IniciarGestion y FinalizarGestion porque ambos pegan a la misma subscription.
        // Una sola instancia (singleton, igual que esta clase) para que los 10 workers
        // (5 Ini + 5 Fin) compitan por el mismo balde en vez de tener 100/min cada uno.
        // SlidingWindow en vez de FixedWindow: un contador que resetea en un instante fijo
        // permite ráfagas de hasta 2x el límite pegadas al borde de la ventana — con un
        // cupo tan ajustado eso alcanza para volver a gatillar el 429. Con cola (en vez de
        // rechazo inmediato) el worker que no tiene turno espera en vez de fallar.
        private readonly RateLimiter _rateLimiter;

        public OracleApiClient(
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration,
            ILogger<OracleApiClient> logger)
        {
            _httpClientFactory = httpClientFactory;
            _configuration = configuration;
            _logger = logger;
            _lockTimeout = TimeSpan.FromSeconds(configuration.GetValue<int>("OracleApi:TokenLockTimeoutSeconds", 30));
            _rateLimiter = new SlidingWindowRateLimiter(new SlidingWindowRateLimiterOptions
            {
                PermitLimit = configuration.GetValue<int>("OracleApi:RateLimitPerMinute", 100),
                Window = TimeSpan.FromMinutes(1),
                SegmentsPerWindow = 6,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                QueueLimit = 1000
            });
        }

        public async Task<(ApiOutcome Outcome, OracleInteraccionResponse? Response)> IniciarGestionAsync(OracleIniLlamadaRequest request, CancellationToken ct)
        {
            var url = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:IniLlamadaPath"]}";
            var subscriptionKey = _configuration["OracleApi:SubscriptionKey"]!;
            var jsonBody = JsonConvert.SerializeObject(request);

            try
            {
                var (outcome, body) = await SendAsync(token =>
                    BuildRequest(HttpMethod.Post, url, token, subscriptionKey, jsonBody), ct);

                if (outcome != ApiOutcome.Success)
                {
                    if (outcome == ApiOutcome.TransientFailure)
                        _logger.LogDebug("IniciarGestion transitorio ({Outcome}), se reintentará en el próximo ciclo: {Body}", outcome, body);
                    else
                        _logger.LogWarning("IniciarGestion no exitoso ({Outcome}): {Body}", outcome, body);
                    return (outcome, null);
                }
                return (ApiOutcome.Success, JsonConvert.DeserializeObject<OracleInteraccionResponse>(body));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "IniciarGestion: fallo no clasificado, se marca como definitivo para revisión manual");
                return (ApiOutcome.PermanentFailure, null);
            }
        }

        public async Task<(ApiOutcome Outcome, string? Body)> FinalizarGestionAsync(OracleFinLlamadaRequest request, long IdOracle, CancellationToken ct)
        {
            var url = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:FinLlamadaPath"]}/{IdOracle}";
            var subscriptionKey = _configuration["OracleApi:SubscriptionKey"]!;
            var jsonBody = JsonConvert.SerializeObject(request);

            try
            {
                var (outcome, body) = await SendAsync(token =>
                    BuildRequest(HttpMethod.Patch, url, token, subscriptionKey, jsonBody), ct);

                if (outcome != ApiOutcome.Success)
                {
                    if (outcome == ApiOutcome.TransientFailure)
                        _logger.LogDebug("FinalizarGestion transitorio en {Url} ({Outcome}), se reintentará en el próximo ciclo: {Body}", url, outcome, body);
                    else
                        _logger.LogWarning("FinalizarGestion no exitoso en {Url} ({Outcome}): {Body}", url, outcome, body);
                    return (outcome, null);
                }
                return (ApiOutcome.Success, body);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FinalizarGestion: fallo no clasificado en {Url}, se marca como definitivo para revisión manual", url);
                return (ApiOutcome.PermanentFailure, null);
            }
        }

        // Handles Polly retries + single token refresh on 401
        private async Task<(ApiOutcome Outcome, string Body)> SendAsync(Func<string, HttpRequestMessage> buildRequest, CancellationToken ct)
        {
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
                    // propio lugar en el balde de 100/min. `ct` se propaga hasta el SendAsync
                    // real y el ReadAsStringAsync, no solo hasta el AcquireAsync, para que un
                    // shutdown grácil corte una request colgada en vez de esperar el timeout
                    // de HttpClient.
                    var (ok, unauthorized, body) = await ResiliencePolicies.HttpRetry.ExecuteAsync<(bool, bool, string)>(async innerCt =>
                    {
                        using var lease = await _rateLimiter.AcquireAsync(1, innerCt);
                        if (!lease.IsAcquired)
                            throw new HttpRequestException("rate limiter: cola llena, se reintentará");

                        using var req = buildRequest(token);
                        var r = await _httpClientFactory.CreateClient().SendAsync(req, innerCt);
                        var b = await r.Content.ReadAsStringAsync(innerCt);
                        if (IsTransient(r.StatusCode))
                            throw new HttpRequestException($"[{(int)r.StatusCode}] {b}");
                        return (r.IsSuccessStatusCode, r.StatusCode == HttpStatusCode.Unauthorized, b);
                    }, ct);

                    if (unauthorized && !tokenRefreshed)
                    {
                        tokenRefreshed = true;
                        await InvalidateTokenAsync();
                        continue;
                    }

                    return (ok ? ApiOutcome.Success : ApiOutcome.PermanentFailure, body);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "SendAsync: fallo transitorio tras agotar reintentos — se reintentará en el próximo ciclo");
                    return (ApiOutcome.TransientFailure, string.Empty);
                }
            }
        }

        public void Dispose() => _rateLimiter.Dispose();

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
                    (isSuccess, _, responseBody) = await ResiliencePolicies.TokenRequestPolicy.ExecuteAsync<(bool, bool, string)>(async innerCt =>
                    {
                        using var req = new HttpRequestMessage(HttpMethod.Post, tokenUrl);
                        req.Headers.Add("Ocp-Apim-Subscription-Key", _configuration["OracleApi:TokenSubscriptionKey"]);
                        req.Headers.Add("clientcredential", _configuration["OracleApi:ClientCredential"]);
                        req.Content = new StringContent(body, Encoding.UTF8, "application/json");

                        var r = await _httpClientFactory.CreateClient().SendAsync(req, innerCt);
                        var b = await r.Content.ReadAsStringAsync(innerCt);
                        if (IsTransient(r.StatusCode))
                            throw new HttpRequestException($"[{(int)r.StatusCode}] {b}");
                        return (r.IsSuccessStatusCode, r.StatusCode == HttpStatusCode.Unauthorized, b);
                    }, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "GetToken: fallo transitorio (rate limit / circuito abierto / red) — se reintentará en el próximo ciclo");
                    return (ApiOutcome.TransientFailure, string.Empty);
                }

                if (!isSuccess)
                {
                    _logger.LogError("Error al obtener token Oracle: {Body}", responseBody);
                    return (ApiOutcome.PermanentFailure, string.Empty);
                }

                var tokenResponse = JsonConvert.DeserializeObject<OracleTokenResponse>(responseBody);
                if (tokenResponse is null)
                {
                    _logger.LogError("Respuesta de token vacía o inválida: {Body}", responseBody);
                    return (ApiOutcome.PermanentFailure, string.Empty);
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

        public string BuildFinLlamadaUrl(long id) =>
            $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:FinLlamadaPath"]}/{id}";

        private static bool IsTransient(HttpStatusCode status) =>
            status == HttpStatusCode.TooManyRequests ||
            status == HttpStatusCode.InternalServerError ||
            status == HttpStatusCode.BadGateway ||
            status == HttpStatusCode.ServiceUnavailable ||
            status == HttpStatusCode.GatewayTimeout;

        private static HttpRequestMessage BuildRequest(HttpMethod method, string url, string token, string subscriptionKey, string jsonBody)
        {
            var request = new HttpRequestMessage(method, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.Add("Ocp-Apim-Subscription-Key", subscriptionKey);
            request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            return request;
        }
    }
}
