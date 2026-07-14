# Migración a Windows Service con auto-recuperación (v2 — validado en producción)

Este documento describe los cambios arquitectónicos para que un Worker Service (.NET
Generic Host + `BackgroundService`) deje de correr como .exe lanzado por Task Scheduler
y pase a ser un Windows Service supervisado, capaz de detectar sus propios hangs y
reiniciarse solo.

**Aplica a cualquier Worker Service con la misma forma** — polling a una BD, procesamiento
en paralelo con workers, llamadas a una API externa. 100% portable a otro proceso con esa
forma. Es la versión corregida después de una migración real: la v1 de este doc tenía
varios supuestos que resultaron equivocados y un bug crítico que solo apareció una vez
en producción. Todo lo marcado como **"Lección real"** viene de haber roto algo de
verdad, no de teoría.

## Por qué

Task Scheduler (y el SCM de Windows, sin lo de abajo) es un lanzador, no un supervisor.
Solo sabe si el proceso existe, no si está haciendo algo útil. Un proceso colgado en un
`await` que nunca vuelve se ve idéntico a uno sano — "Running" no significa "trabajando".

**Aviso importante antes de empezar**: migrar a Windows Service SIN el heartbeat/watchdog
de más abajo NO resuelve este problema — el SCM hace el mismo chequeo superficial que
Task Scheduler. El watchdog es la pieza que realmente lo arregla, no el hosting.

## Cambio 1 — Hosting como Windows Service

```bash
dotnet add package Microsoft.Extensions.Hosting.WindowsServices --version 9.0.9
```

```csharp
private static IHostBuilder CreateHostBuilder(string[] args, IConfiguration configuration) =>
    Host.CreateDefaultBuilder(args)
        .UseWindowsService(options => options.ServiceName = "NombreDelServicio")
        .UseSerilog()
        // ... resto de la config existente
```

El mismo binario corre con `dotnet run` en desarrollo y como servicio en el servidor —
`UseWindowsService()` solo adapta el ciclo de vida cuando detecta que lo está controlando
el SCM (no pasa nada en modo consola/debug).

## Cambio 2 — Comportamiento explícito ante excepciones no manejadas

```csharp
services.Configure<HostOptions>(o =>
{
    o.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.StopHost;
    o.ShutdownTimeout = TimeSpan.FromSeconds(15);
});
```

`ShutdownTimeout` es nuevo en esta versión — acota cuánto espera el Host un shutdown
grácil antes de forzar. Sin esto, el default puede dejar el proceso colgado en el
apagado si algún `BackgroundService` no respeta bien el `CancellationToken`.

**Lección real**: bajo `StopHost`, `host.RunAsync()` vuelve de forma normal, no relanza
la excepción. Pero **también puede volver por otras razones** — en producción vimos un
`System.OperationCanceledException` propagarse desde `WindowsServiceLifetime.StopAsync`
hasta el `catch` de `Main` cuando el SCM mandó una señal de parada externa (alguien hizo
`sc stop`, o un reinicio del server). El proceso lo logueó como "Error fatal" cuando en
realidad fue un shutdown normal — el Recovery del SCM lo levantó bien igual, pero el log
es confuso. Si ves esto en tus logs, no asumas que fue un crash real sin revisar el
stack trace primero.

## Cambio 3 — Eliminar retry loops manuales, delegar el reinicio al SCM

**Antes** (antipatrón a buscar y sacar):
```csharp
while (true)
{
    try
    {
        var host = CreateHostBuilder(args, configuration).Build();
        await host.RunAsync();
        break; // BUG: esto se ejecuta también cuando el host se detiene por StopHost
    }
    catch (Exception ex)
    {
        Log.Fatal(ex, "reiniciando en 30 segundos...");
        await Task.Delay(TimeSpan.FromSeconds(30));
    }
}
```

**Después**:
```csharp
try
{
    Log.Information("Iniciando...");
    var host = CreateHostBuilder(args, configuration).Build();
    await host.RunAsync();
    Log.Information("Aplicación detenida correctamente");
}
catch (Exception ex)
{
    Log.Fatal(ex, "Error fatal al iniciar o ejecutar el host");
    Log.CloseAndFlush();
    Environment.Exit(1);
}

Log.CloseAndFlush();
```

Dos mecanismos de reintento compitiendo generan comportamiento confuso. Con el SCM como
único responsable de reiniciar, hay un solo lugar donde ver el historial de caídas.

## Cambio 4 — Heartbeat + Watchdog (reescrito — la v1 tenía un bug crítico)

### El diseño de un solo heartbeat NO alcanza

La v1 de este doc proponía un único `IHeartbeatMonitor` compartido. **Dos problemas reales
aparecieron con eso**, ambos encontrados recién en producción, no en el diseño:

**Problema 1 — heartbeat ciego a hangs de consumers.** Si tenés un Producer (polling) y
N Consumers (procesan items en paralelo vía `Channel<T>`), un solo heartbeat reportado
solo por el Producer no detecta que los Consumers se colgaron — el Producer sigue
polleando fino sin depender de que los Consumers progresen.

**Problema 2 (el más grave, encontrado en producción real) — heartbeat que nunca se
refresca cuando NO hay trabajo.** Si el código de un Consumer solo llama
`ReportAlive()` DENTRO del loop que procesa items (ej. `await foreach` sobre un
`Channel.Reader.ReadAllAsync()`), y el channel está vacío por un rato (de noche, fin
de semana, poca actividad), el cuerpo del loop nunca se ejecuta — **el heartbeat
nunca se refresca aunque no haya nada mal**. El Watchdog interpreta "no hay heartbeat"
como "está colgado" y fuerza un reinicio cada vez que se cumple el umbral, aunque el
proceso esté perfectamente sano. Esto puede ser **peor que el problema original**: en
vez de un hang real sin detectar, tenés reinicios falsos-positivos recurrentes.

### Diseño correcto: dos carriles de heartbeat, con "alive" independiente del trabajo disponible

**`IHeartbeatMonitor.cs`**
```csharp
public interface IHeartbeatMonitor
{
    void ReportAlive();
    DateTime LastHeartbeatUtc { get; }

    // Separado de "alive" a propósito — ver más abajo por qué hacen falta los dos.
    void ReportProgress();
    DateTime LastProgressUtc { get; }
}
```

**`HeartbeatMonitor.cs`**
```csharp
public class HeartbeatMonitor : IHeartbeatMonitor
{
    private long _lastHeartbeatTicks = DateTime.UtcNow.Ticks;
    private long _lastProgressTicks = DateTime.UtcNow.Ticks;

    public void ReportAlive() =>
        Interlocked.Exchange(ref _lastHeartbeatTicks, DateTime.UtcNow.Ticks);
    public DateTime LastHeartbeatUtc =>
        new(Interlocked.Read(ref _lastHeartbeatTicks), DateTimeKind.Utc);

    public void ReportProgress() =>
        Interlocked.Exchange(ref _lastProgressTicks, DateTime.UtcNow.Ticks);
    public DateTime LastProgressUtc =>
        new(Interlocked.Read(ref _lastProgressTicks), DateTimeKind.Utc);
}
```

**Dos instancias vía DI keyed services** (una por rol — Producer y Consumers son
conceptualmente distintos, no comparten la misma instancia):
```csharp
services.AddKeyedSingleton<IHeartbeatMonitor, HeartbeatMonitor>("Producer");
services.AddKeyedSingleton<IHeartbeatMonitor, HeartbeatMonitor>("Consumers");
```

Inyectás con `[FromKeyedServices("Producer")]` / `[FromKeyedServices("Consumers")]` en
el constructor de quien corresponda.

### `ReportAlive()` — debe fijarse INDEPENDIENTE de si hay trabajo o no

**Antitpatrón (el bug de producción)**:
```csharp
await foreach (var item in channel.Reader.ReadAllAsync(ct))
{
    // ... procesar ...
    heartbeat.ReportAlive(); // nunca se ejecuta si el channel está vacío
}
```

**Correcto** — usar `WaitToReadAsync` con un timeout corto, y reportar "vivo" en la
espera misma, no solo al procesar:
```csharp
while (!ct.IsCancellationRequested)
{
    bool hasData;
    try
    {
        var waitTask = channel.Reader.WaitToReadAsync(ct).AsTask();
        var idleTask = Task.Delay(TimeSpan.FromSeconds(15), ct);
        if (await Task.WhenAny(waitTask, idleTask) == idleTask)
        {
            // Sin trabajo pendiente hace un rato — no es un hang, no hay nada que
            // hacer. El worker sigue vivo y al día.
            heartbeat.ReportAlive();
            heartbeat.ReportProgress();
            continue;
        }
        hasData = await waitTask;
    }
    catch (OperationCanceledException) { break; }

    if (!hasData) break; // el productor cerró el channel

    while (channel.Reader.TryRead(out var item))
    {
        // ... procesar item ...
        heartbeat.ReportAlive();
    }
}
```

El intervalo del `idleTask` (acá 15s) tiene que ser cómodamente menor al `checkInterval`
del Watchdog, para que el heartbeat siempre esté fresco cuando el Watchdog lo revisa.

**Lección real — el loop que drena el channel también necesita chequear cancelación,
no solo el loop externo.** El patrón de arriba tiene DOS loops: el externo (`while
(!ct.IsCancellationRequested)`) y uno interno que drena todo lo que ya está en cola:
```csharp
while (_iniChannel.Reader.TryRead(out var record))
{
    using var scope = _serviceProvider.CreateScope(); // ← acá revienta
    // ... procesar ...
}
```
Si el `while` interno **no** chequea `ct.IsCancellationRequested`, y el shutdown arranca
justo cuando quedan varios ítems en cola, el loop los sigue drenando todos igual — y cada
uno choca contra el `IServiceProvider` raíz, que el Host ya empezó a disponer como parte
del apagado (`System.ObjectDisposedException: Cannot access a disposed object. Object
name: 'IServiceProvider'`). Resultado en producción: una ráfaga de decenas de excepciones
idénticas, todas con el mismo timestamp exacto — un solo evento de shutdown, no un
problema de negocio. El fix es agregar el chequeo también acá:
```csharp
while (!ct.IsCancellationRequested && _iniChannel.Reader.TryRead(out var record))
```
Como el `CancellationToken` de un `BackgroundService` está atado a
`IHostApplicationLifetime.ApplicationStopping`, y `HostOptions.ShutdownTimeout` (Cambio 2)
le da una ventana antes de que el contenedor se dispose de verdad, este chequeo alcanza
para cortar prolijo sin necesidad de capturar `ObjectDisposedException` a mano.

### `ReportProgress()` — solo en un resultado DEFINITIVO, no en cualquier intento

Si tu proceso llama a una API externa con reintentos/circuit breaker (ver Cambio 6+),
vas a tener un tercer estado además de éxito/fracaso: "fallo transitorio, se reintenta
solo" (ver más abajo). `ReportProgress()` NO debe dispararse en ese caso — solo en
éxito o fracaso definitivo:
```csharp
var outcome = await ProcessItem(item);
if (outcome != Outcome.TransientFailure)
    heartbeat.ReportProgress();
heartbeat.ReportAlive(); // esto sí, siempre que el intento terminó sin colgarse
```

**Por qué separar "alive" de "progress"**: si una API externa está mal de forma
SOSTENIDA (no un blip de segundos, sino minutos u horas), cada worker puede seguir
"intentando y fallando prolijamente" en loop, reportando `ReportAlive()` cada vez
(correcto — el worker no está colgado) pero SIN `ReportProgress()` (correcto también
— no se está logrando nada real). El Watchdog necesita chequear los dos umbrales por
separado: uno corto para detectar hangs reales, uno mucho más largo para detectar
"vivo pero sin lograr nada" sostenido.

**`WatchdogHostedService.cs`** (fragmento relevante):
```csharp
var elapsedProducer = DateTime.UtcNow - _producerHeartbeat.LastHeartbeatUtc;
var elapsedConsumers = DateTime.UtcNow - _consumersHeartbeat.LastHeartbeatUtc;
var elapsedProducerProgress = DateTime.UtcNow - _producerHeartbeat.LastProgressUtc;
var elapsedConsumersProgress = DateTime.UtcNow - _consumersHeartbeat.LastProgressUtc;

var heartbeatVencido = elapsedProducer > _staleThreshold || elapsedConsumers > _staleThreshold;
var sinProgresoReal = elapsedProducerProgress > _maxNoProgress || elapsedConsumersProgress > _maxNoProgress;

if (heartbeatVencido || sinProgresoReal)
{
    // ver Cambio 4b — cómo reaccionar
}
```

### Cómo reaccionar — NO usar `Environment.FailFast`

**Antipatrón (v1 de este doc)**: `Log.CloseAndFlush(); Environment.FailFast(...)`.

Dos problemas reales: (1) `Log.CloseAndFlush()` desde el thread del Watchdog compite
con otros `BackgroundService` que siguen logueando al mismo `Log.Logger` estático —
race real, puede perder justo el log que explica el reinicio. (2) `FailFast` salta el
shutdown grácil del Host (trabajo en curso se corta de golpe) y genera un evento de
Windows Error Reporting en cada disparo — ruido operativo innecesario para algo que
es una decisión deliberada del propio watchdog, no una corrupción real detectada
por el CLR.

**Correcto**:
```csharp
Environment.ExitCode = 1;
_lifetime.StopApplication(); // requiere IHostApplicationLifetime inyectado
return;
```

Esto pide un shutdown grácil (acotado por `HostOptions.ShutdownTimeout`, Cambio 2), y
deja que el ÚNICO `Log.CloseAndFlush()` pase al final de `Main`, cuando `RunAsync()` ya
volvió y no quedan otros `BackgroundService` escribiendo — sin la race del punto (1).
`Environment.ExitCode` queda seteado para que el proceso salga con código != 0 y el SCM
aplique la política de Recovery.

### El umbral no puede ser un multiplicador ciego del polling, y hay que recalcularlo si algo cambia

```csharp
var minThresholdSeconds = configuration.GetValue<int>("Watchdog:MinStaleThresholdSeconds", <recalcular>);
_staleThreshold = TimeSpan.FromSeconds(Math.Max(pollingSeconds * staleMultiplier, minThresholdSeconds));
```

Fórmula: `(intentos totales de tu retry policy) x CommandTimeout + (reintentos) x delay
de espera`, multiplicado por cuántas llamadas secuenciales hace tu ciclo de polling en
la peor vuelta. **Lección real**: este cálculo se hizo mal la primera vez (se asumió
`CommandTimeout` default de ADO.NET = 30s y delay de Polly = 1s, cuando la connection
string real tenía `Command Timeout=10` explícito y el delay real configurado era 2s) —
el número resultante quedó 2.7x más alto de lo necesario, sin margen real. **Verificá
los valores REALES de tu `appsettings.json` y de tu policy de Polly antes de calcular
esto — no copies un número de otro proyecto sin rehacer la cuenta.**

`MaxNoProgressMinutes` (el umbral largo, para el escenario "vivo pero sin progreso
real"): no hay una fórmula exacta acá — es un juicio de "cuánto tiempo de degradación
sostenida de una dependencia externa es razonable tolerar antes de forzar un reinicio".
15 minutos fue el valor elegido en este proyecto; ajustalo según qué tan crítico sea
el SLA de tu proceso.

## Cambio 5 — `PooledConnectionLifetime` en el HttpClient

```csharp
services.AddHttpClient();
services.ConfigureHttpClientDefaults(builder =>
    builder.ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5)
    }));
```

`SocketsHttpHandler` por default reusa conexiones TCP/TLS para siempre. Si el backend
externo rota de IP (balanceo, deploy, API Management), el proceso puede seguir mandando
tráfico a una conexión pooled que apunta a un backend muerto. Usar
`ConfigureHttpClientDefaults` (no `.ConfigurePrimaryHttpMessageHandler` encadenado
directo sobre `AddHttpClient()`) evita un problema de resolución de overload en
compilación, y aplica la config a todos los clients registrados.

## Cambio 6 — Timeout en locks compartidos + clasificación correcta del resultado

Si tenés un recurso compartido protegido por un `SemaphoreSlim` entre múltiples workers
concurrentes (ej. un token de autenticación cacheado), nunca lo esperes sin timeout:

```csharp
if (!await _lock.WaitAsync(_lockTimeout))
    throw new TimeoutException("No se pudo obtener el lock a tiempo — otro worker lo tiene retenido.");
```

**Lección real — esto solo, sin lo de abajo, introduce pérdida de datos silenciosa.**
Si el código que llama a esto trata CUALQUIER excepción como "fallo definitivo" (y
persiste eso como un estado terminal en la BD, ej. `Envio=3` que nunca se vuelve a
reintentar), un timeout de lock transitorio —algo que antes solo demoraba— ahora
**pierde el registro para siempre**. Hace falta un tercer estado explícito:

```csharp
public enum ApiOutcome { Success, TransientFailure, PermanentFailure }
```

`TransientFailure` (timeout de lock, rate limit, 5xx agotado) → el caller NO marca el
registro como fallido, lo deja para el próximo ciclo. Solo `PermanentFailure` (rechazo
de negocio real, o un error no clasificado que sí conviene loguear fuerte) marca el
estado terminal. Ver también Cambio 6b para el caso específico de rate limiting.

**Todo lugar donde se escribe el recurso compartido tiene que tomar el MISMO lock** —
en este proyecto se encontró un método (`InvalidateToken`) que escribía el token sin
tomar el lock que sí tomaba el método de lectura — race de datos real, no teórico.

## Cambio 6b — Rate limiting sostenido: Circuit Breaker, no solo retry

Si el recurso protegido por el lock del Cambio 6 es una llamada a una API externa con
rate limit (HTTP 429), un retry simple no alcanza — **el lock amplifica el problema**:
si el worker que tiene el lock falla contra un endpoint rate-limiteado, cada worker que
queda en fila detrás reintenta la MISMA llamada fallida desde cero, en serie, lo que
puede convertir un rate limit de segundos en una ventana de 40-60+ segundos donde el
proceso parece completamente colgado (aunque técnicamente no lo está).

**Fix con Polly (ya suele estar en el proyecto, no hace falta paquete nuevo si ya usás
Polly para retries)**:
```csharp
internal static readonly IAsyncPolicy Retry =
    Policy.Handle<HttpRequestException>().Or<TaskCanceledException>()
        .WaitAndRetryAsync(2, retryAttempt =>
            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)) + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500)));

internal static readonly IAsyncPolicy CircuitBreaker =
    Policy.Handle<HttpRequestException>()
        .CircuitBreakerAsync(exceptionsAllowedBeforeBreaking: 2, durationOfBreak: TimeSpan.FromSeconds(15));

internal static readonly IAsyncPolicy RequestPolicy = Policy.WrapAsync(Retry, CircuitBreaker);
```

Orden importa: `Retry` afuera, `CircuitBreaker` adentro (`WrapAsync(outer, inner)`).
Cuando el circuito está abierto, la excepción que tira (`BrokenCircuitException`, que
NO hereda de `HttpRequestException`) sale sin que `Retry` la reintente — falla rápido
en vez de repetir la llamada de red. Efecto: después de que 1-2 workers confirman que
el endpoint está mal, el resto de la fila falla en milisegundos en vez de repetir la
llamada real — la ventana de "parece colgado" baja de 40-60s a unos pocos segundos.
Backoff exponencial con jitter (en vez de un delay fijo) evita que todos los workers
reintenten exactamente al mismo instante.

## Cambio 7 — Exponer timings en `appsettings`, no solo como default en código

Todo valor de timing/umbral que pueda necesitar ajuste en producción sin recompilar
debe estar en `appsettings.{Environment}.json`. `configuration.GetValue<int>("Clave",
defaultSiNoExiste)` funciona igual esté o no la clave en el JSON — pero si no está,
queda invisible para cualquiera que no lea el código fuente.

**Lección real**: mantené el `appsettings.example.json` (documentación, sin secretos
reales) sincronizado con los valores REALES del `appsettings.json` de producción — en
este proyecto quedaron desincronizados (`PollingIntervalSeconds` distinto, un umbral
recalculado que nunca se aplicó al archivo real) durante buena parte de la migración,
y nadie lo notó hasta una revisión explícita.

## Cambio 8 — Dónde vive `appsettings.json`, y por qué importa

**`appsettings.json` (con secretos reales) debe vivir SOLO en el composition root** —
el proyecto que se ejecuta (`OutputType=Exe`), nunca en una class library de
Infraestructura/Dominio. Una class library no debería tener su propia copia de
credenciales de producción — es superficie de fuga sin ningún beneficio funcional.

Para que no haga falta copiarlo a mano al `bin/` en cada build:
```xml
<ItemGroup>
  <None Include="appsettings.json" Condition="Exists('appsettings.json')">
    <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
  </None>
</ItemGroup>
```
(`Microsoft.NET.Sdk` plano, a diferencia de `Microsoft.NET.Sdk.Web`, no copia
`appsettings.json` al output automáticamente — hay que declararlo).

## Cambio 9 — Un Windows Service no arranca con un directorio de trabajo útil

Si usás rutas relativas en algún sink de logging (`Serilog.Sinks.File`, por ejemplo),
tené en cuenta que el CWD de un Windows Service puede ser `C:\Windows\System32` u otro
valor no intuitivo, no la carpeta del `.exe`. Fijalo explícito, reusando la misma
lógica que ya tengas para distinguir dev/prod:

```csharp
string basePath = Debugger.IsAttached ? AppContext.BaseDirectory : @"C:\RutaDeployment";
Environment.CurrentDirectory = basePath; // antes de construir la configuración/logger
```

## Cambio 10 — Logging: separar por destino y nivel, y solo loguear lo accionable

- **Un sink dependiente de BD no es suficiente** — si la BD es justo la que está fallando
  (motivo típico de necesitar ver el log), un `LogCritical` de ese incidente puede no
  tener ningún respaldo. Agregar un sink de archivo local en paralelo, independiente
  de la salud de la BD.
- **Separar por nivel entre sinks, no duplicar todo en los dos**: un sink de alerta
  (BD, `restrictedToMinimumLevel: Warning`) para lo accionable; un sink de archivo
  (`Debug`/`Information`, filtrado por expresión con `Serilog.Expressions` +
  `Filter.ByIncludingOnly`) para visibilidad de detalle sin llenar la tabla de alertas
  con ruido.
- **No loguear el camino feliz por ítem.** Si procesás muchos ítems y la mayoría
  tienen éxito, no loguees "empezando a procesar X" ni "X completado con éxito" por
  cada uno — es la mayoría de las líneas y no aporta señal. Logueá solo fallas
  (transitorias en `Debug`, definitivas en `Information`/`Warning`) y eventos de
  ciclo de vida/agregados ("N pendientes esta vuelta").
- **PII**: si logueás datos personales (teléfonos, DNI, emails) como propiedades
  estructuradas, enmascaralos antes de empujarlos al `LogContext` — terminan en texto
  plano en disco y en tablas de BD potencialmente con menos control de acceso que la
  tabla de negocio.
- **`GETDATE()` del SQL Server puede no coincidir con la hora local del negocio.**
  Si usás `GETDATE()` para calcular ventanas de fecha ("registros de hoy"), verificá
  con `SELECT GETDATE()` contra la hora local real antes de confiar en el resultado —
  un server con el reloj corrido (ej. UTC en vez de la zona horaria del negocio) hace
  que la ventana de "hoy" se calcule mal durante varias horas cada día, y los registros
  afectados pueden quedar permanentemente fuera del filtro. Si hay desfasaje,
  compensarlo explícito: `CAST(DATEADD(HOUR, -N, GETDATE()) AS DATE)`.
- **Queries con `CONVERT(columna, ...)` en el `WHERE` no son sargables** — SQL Server
  no puede usar ningún índice sobre esa columna, fuerza un scan completo en cada
  llamada. Preferir comparaciones de rango contra la columna cruda (`columna >= X AND
  columna < Y`) en vez de envolverla en una función.

## Checklist de deploy (infraestructura, no código)

1. `dotnet publish` (Release) al servidor.
2. Copiar el `appsettings.json` real de producción a la carpeta de deploy.
3. `sc.exe create <NombreServicio> binPath= "<ruta>\<exe>.exe" start= auto obj= "DOMINIO\usuario" password= "..."`
   (espacio después de cada `=` obligatorio). Si la cuenta es de dominio y el server
   tiene GPO propia, el derecho "Log on as a service" puede pisarse solo en el próximo
   `gpupdate` — si ves Error 1069 después de que andaba bien, ese es el motivo, hay
   que pedirle al admin de dominio que lo agregue a nivel GPO, no a `secpol.msc` local.
4. `icacls "<ruta>" /grant "DOMINIO\usuario:(OI)(CI)RX"`.
5. `sc.exe failure <NombreServicio> reset= 86400 actions= restart/60000/restart/60000/restart/60000`
   — sin esto, el Watchdog mata el proceso para nada, porque nadie lo revive. `reset=`
   es config persistente (no hay que re-ejecutar el comando en cada reinicio) — resetea
   el contador de fallas solo tras 24hs corridas sin fallar.
6. **Dar de baja la Scheduled Task vieja** antes de instalar el servicio — si conviven,
   procesan todo duplicado.
7. Verificar que la cuenta de servicio tenga la contraseña configurada para no expirar
   (o un proceso de rotación conocido) — si expira, el servicio deja de poder loguearse
   y ningún watchdog te salva de eso, es una categoría de falla distinta.

## Validar después del deploy — esto SÍ se hizo en esta migración

A diferencia de la v1 de este documento (que decía "nunca se verificó empíricamente"),
en esta migración el mecanismo completo se validó con un incidente real: el proceso se
cortó por una señal externa, el log lo mostró como "Error fatal" (era un
`OperationCanceledException` de shutdown, no un crash real — ver Cambio 2), y **74
segundos después** el SCM lo reinició solo, confirmado con el timestamp del próximo
"Watchdog iniciado" en el log. El mecanismo funciona. Aun así, antes de confiar
ciegamente en un proyecto nuevo:

- Provocá un hang real (cortar la BD/API externa un rato largo, más que el umbral
  configurado) y confirmá que el Watchdog dispara y el SCM levanta el proceso solo.
- Confirmá que el escenario del Cambio 4 ("vivo pero sin trabajo") no dispara un
  reinicio falso — dejalo corriendo sin actividad más tiempo que el `checkInterval`
  del Watchdog y confirmá que NO se reinicia.
- Confirmá que un rate limit sostenido de la API externa (si aplica) se recupera solo
  sin perder registros — revisá que no queden filas en un estado terminal que
  correspondan a fallas transitorias.

## Al portar esto a otro proyecto — no copiar a ciegas

- Recalculá los umbrales del Watchdog según los timeouts/reintentos REALES de ese
  proyecto (Cambio 4) — no copies un número de acá.
- Confirmá que el heartbeat de cada worker se refresca aunque no haya trabajo
  disponible (Cambio 4) — es el bug más caro de este documento, probalo explícito.
- Revisá si ese proyecto tiene su propio lock compartido sin timeout (Cambio 6), y si
  llama a una API externa con posibilidad de rate limit (Cambio 6b).
- Auditá dead code al terminar: campos inyectados y nunca leídos, columnas de BD leídas
  y nunca usadas, paquetes NuGet sin referencias reales — la refactorización de hosting
  suele dejar residuos de versiones anteriores del arranque de la app.
- Verificá la hora del SQL Server real contra la hora de negocio si hay filtros de
  fecha (Cambio 10) — no asumas que coinciden.
