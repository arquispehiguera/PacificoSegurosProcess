using Microsoft.Data.SqlClient;
using Polly;

namespace PacificoSeguros.Infraestructure;

internal static class ResiliencePolicies
{
    internal static readonly IAsyncPolicy DbRetry =
        Policy
            .Handle<SqlException>()
            .Or<TimeoutException>()
            .WaitAndRetryAsync(2, _ => TimeSpan.FromSeconds(2));

    internal static readonly IAsyncPolicy HttpRetry =
        Policy
            .Handle<HttpRequestException>()
            .Or<TaskCanceledException>()
            .WaitAndRetryAsync(2, retryAttempt =>
                TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500)));

    internal static readonly IAsyncPolicy TokenCircuitBreaker =
        Policy
            .Handle<HttpRequestException>()
            .CircuitBreakerAsync(exceptionsAllowedBeforeBreaking: 2, durationOfBreak: TimeSpan.FromSeconds(15));

    internal static readonly IAsyncPolicy TokenRequestPolicy =
        Policy.WrapAsync(HttpRetry, TokenCircuitBreaker);
}
