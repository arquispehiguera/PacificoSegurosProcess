using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;
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
            var token = await GetTokenAsync();
            var url = $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:IniLlamadaPath"]}";
            var subscriptionKey = _configuration["OracleApi:SubscriptionKey"]!;
            using var httpRequest = BuildRequest(HttpMethod.Post, url, token, subscriptionKey, JsonConvert.SerializeObject(request));
            try
            {
                var response = await _httpClientFactory.CreateClient().SendAsync(httpRequest);
                var body = await response.Content.ReadAsStringAsync();
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("IniciarGestion falló [{Status}]: {Body}", (int)response.StatusCode, body);
                    return null;
                }
                return JsonConvert.DeserializeObject<OracleInteraccionResponse>(body);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error HTTP en IniciarGestion");
                return null;
            }
        }

        public async Task<string?> FinalizarGestionAsync(string urlOracle, OracleFinLlamadaRequest request)
        {
            var token = await GetTokenAsync();
            var subscriptionKey = _configuration["OracleApi:SubscriptionKey"]!;
            using var httpRequest = BuildRequest(HttpMethod.Patch, urlOracle, token, subscriptionKey, JsonConvert.SerializeObject(request));
            try
            {
                var response = await _httpClientFactory.CreateClient().SendAsync(httpRequest);
                var body = await response.Content.ReadAsStringAsync();
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("FinalizarGestion falló [{Status}] en {Url}: {Body}", (int)response.StatusCode, urlOracle, body);
                    return null;
                }
                return body;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error HTTP en FinalizarGestion para {Url}", urlOracle);
                return null;
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
                using var request = new HttpRequestMessage(HttpMethod.Post, tokenUrl);
                request.Headers.Add("Ocp-Apim-Subscription-Key", _configuration["OracleApi:TokenSubscriptionKey"]);
                request.Headers.Add("clientcredential", _configuration["OracleApi:ClientCredential"]);
                request.Content = new StringContent(body, Encoding.UTF8, "application/json");

                var response = await _httpClientFactory.CreateClient().SendAsync(request);
                var responseBody = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                    throw new InvalidOperationException($"Error al obtener token Oracle: {(int)response.StatusCode} - {responseBody}");

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

        public string BuildFinLlamadaUrl(long id) =>
            $"{_configuration["OracleApi:BaseUrl"]}{_configuration["OracleApi:FinLlamadaPath"]}/{id}";

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
