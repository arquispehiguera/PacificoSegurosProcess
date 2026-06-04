using Microsoft.Data.SqlClient;
using Polly;

namespace PacificoSeguros.Infraestructure;

internal static class ResiliencePolicies
{
    internal static readonly IAsyncPolicy DbRetry =
        Policy
            .Handle<SqlException>()
            .Or<TimeoutException>()
            .WaitAndRetryAsync(2, _ => TimeSpan.FromSeconds(1));

    internal static readonly IAsyncPolicy HttpRetry =
        Policy
            .Handle<HttpRequestException>()
            .Or<TaskCanceledException>()
            .WaitAndRetryAsync(2, _ => TimeSpan.FromSeconds(1));
}
