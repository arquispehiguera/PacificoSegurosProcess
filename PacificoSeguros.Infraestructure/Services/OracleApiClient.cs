using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;
using System.Net;
using System.Net.Http.Headers;
using System.Text;

namespace PacificoSeguros.Infraestructure.Services
{
    public class OracleApiClient : IOracleApiClient
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<OracleApiClient> _logger;

        private string? _accessToken;
        private DateTime _tokenExpiresAt = DateTime.MinValue;
        private readonly SemaphoreSlim _tokenLock = new(1, 1);

        public OracleApiClient(
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration,
            ILogger<OracleApiClient> logger)
        {
            _httpClientFactory = httpClientFactory;
            _configuration = configuration;
            _logger = logger;
        }

        public async Task<OracleInteraccionResponse?> IniciarGestionAsync(OracleIniLlamadaRequest request)
        {
            var url = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:IniLlamadaPath"]}";
            var subscriptionKey = _configuration["OracleApi:SubscriptionKey"]!;
            var jsonBody = JsonConvert.SerializeObject(request);

            try
            {
                var (ok, body) = await SendAsync(token =>
                    BuildRequest(HttpMethod.Post, url, token, subscriptionKey, jsonBody));

                if (!ok)
                {
                    _logger.LogError("IniciarGestion falló: {Body}", body);
                    return null;
                }
                return JsonConvert.DeserializeObject<OracleInteraccionResponse>(body);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "IniciarGestion falló después de 2 reintentos");
                return null;
            }
        }

        public async Task<string?> FinalizarGestionAsync(OracleFinLlamadaRequest request, long IdOracle)
        {
            var url = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:FinLlamadaPath"]}/{IdOracle}";
            var subscriptionKey = _configuration["OracleApi:SubscriptionKey"]!;
            var jsonBody = JsonConvert.SerializeObject(request);

            try
            {
                var (ok, body) = await SendAsync(token =>
                    BuildRequest(HttpMethod.Patch, url, token, subscriptionKey, jsonBody));

                if (!ok)
                {
                    _logger.LogError("FinalizarGestion falló en {Url}: {Body}", url, body);
                    return null;
                }
                return body;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "FinalizarGestion falló después de 2 reintentos para {Url}", url);
                return null;
            }
        }

        // Handles Polly retries + single token refresh on 401
        private async Task<(bool ok, string body)> SendAsync(Func<string, HttpRequestMessage> buildRequest)
        {
            bool tokenRefreshed = false;
            while (true)
            {
                var token = await GetTokenAsync();

                var (ok, unauthorized, body) = await ResiliencePolicies.HttpRetry.ExecuteAsync<(bool, bool, string)>(async () =>
                {
                    using var req = buildRequest(token);
                    var r = await _httpClientFactory.CreateClient().SendAsync(req);
                    var b = await r.Content.ReadAsStringAsync();
                    if (IsTransient(r.StatusCode))
                        throw new HttpRequestException($"[{(int)r.StatusCode}] {b}");
                    return (r.IsSuccessStatusCode, r.StatusCode == HttpStatusCode.Unauthorized, b);
                });

                if (unauthorized && !tokenRefreshed)
                {
                    tokenRefreshed = true;
                    InvalidateToken();
                    continue;
                }

                return (ok, body);
            }
        }

        private async Task<string> GetTokenAsync()
        {
            await _tokenLock.WaitAsync();
            try
            {
                if (!string.IsNullOrEmpty(_accessToken) && DateTime.UtcNow < _tokenExpiresAt)
                    return _accessToken;

                _logger.LogInformation("Obteniendo token Oracle...");

                var body = JsonConvert.SerializeObject(new { resource = _configuration["OracleApi:Resource"] });
                var tokenUrl = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:TokenPath"]}";

                bool isSuccess;
                string responseBody;

                try
                {
                    (isSuccess, _, responseBody) = await ResiliencePolicies.HttpRetry.ExecuteAsync<(bool, bool, string)>(async () =>
                    {
                        using var req = new HttpRequestMessage(HttpMethod.Post, tokenUrl);
                        req.Headers.Add("Ocp-Apim-Subscription-Key", _configuration["OracleApi:TokenSubscriptionKey"]);
                        req.Headers.Add("clientcredential", _configuration["OracleApi:ClientCredential"]);
                        req.Content = new StringContent(body, Encoding.UTF8, "application/json");

                        var r = await _httpClientFactory.CreateClient().SendAsync(req);
                        var b = await r.Content.ReadAsStringAsync();
                        if (IsTransient(r.StatusCode))
                            throw new HttpRequestException($"[{(int)r.StatusCode}] {b}");
                        return (r.IsSuccessStatusCode, r.StatusCode == HttpStatusCode.Unauthorized, b);
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "GetToken falló después de 2 reintentos");
                    throw;
                }

                if (!isSuccess)
                    throw new InvalidOperationException($"Error al obtener token Oracle: {responseBody}");

                var tokenResponse = JsonConvert.DeserializeObject<OracleTokenResponse>(responseBody)
                    ?? throw new InvalidOperationException("Respuesta de token vacía o inválida");

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

                return _accessToken;
            }
            finally
            {
                _tokenLock.Release();
            }
        }

        private void InvalidateToken()
        {
            _accessToken = null;
            _tokenExpiresAt = DateTime.MinValue;
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
