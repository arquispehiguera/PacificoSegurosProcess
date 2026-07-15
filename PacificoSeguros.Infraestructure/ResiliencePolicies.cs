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

    // Para persistir un resultado ya confirmado por Oracle (Success con IdOracle en
    // mano, o PermanentFailure) — más paciente que DbRetry porque acá no hay margen
    // para simplemente perder el dato: si Oracle dijo que sí y no logramos guardarlo,
    // la fila queda en Envio=2 sin ningún rastro de que ya fue procesada, y alguien
    // podría reenviarla pensando que nunca salió. 5 intentos, 5s cada uno.
    internal static readonly IAsyncPolicy PersistConfirmedResultRetry =
        Policy
            .Handle<SqlException>()
            .Or<TimeoutException>()
            .WaitAndRetryAsync(5, _ => TimeSpan.FromSeconds(5));

    // Corta la insistencia si la base realmente dejó de responder, en vez de que cada
    // uno de los workers concurrentes queme sus 25s de reintento por separado — el
    // conteo de fallos es compartido (política singleton), así que 3 fallos en total
    // (pueden ser todos del mismo ítem, o repartidos entre varios workers) abren el
    // circuito para todos durante 30s, sin seguir golpeando una base ya saturada.
    internal static readonly IAsyncPolicy PersistCircuitBreaker =
        Policy
            .Handle<SqlException>()
            .Or<TimeoutException>()
            .CircuitBreakerAsync(exceptionsAllowedBeforeBreaking: 3, durationOfBreak: TimeSpan.FromSeconds(30));

    internal static readonly IAsyncPolicy PersistConfirmedResultPolicy =
        Policy.WrapAsync(PersistConfirmedResultRetry, PersistCircuitBreaker);
}
