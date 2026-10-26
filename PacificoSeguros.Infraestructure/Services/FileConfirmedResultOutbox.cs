using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using PacificoSeguros.Core.Entities;
using PacificoSeguros.Core.Interfaces;

namespace PacificoSeguros.Infraestructure.Services
{
    // Outbox en disco local: un archivo JSON por LastInteractionId. Se eligió disco y
    // no otra tabla porque el escenario que protege es justamente SQL Server caído —
    // cualquier respaldo dentro de la misma base fallaría en el mismo momento.
    //
    // Sin estado compartido más allá de la ruta: cada worker escribe y borra solo el
    // archivo de su propio LastInteractionId, así que no hace falta lock entre workers.
    // El mantenimiento y el paso a expired/ los ejecuta solo el Producer (un único hilo).
    public class FileConfirmedResultOutbox : IConfirmedResultOutbox
    {
        private const string FileExtension = ".json";
        private const string TempExtension = ".tmp";
        private const string ExpiredFolder = "expired";
        private const string QuarantineFolder = "quarantine";

        private readonly string _directory;
        private readonly string _expiredDirectory;
        private readonly string _quarantineDirectory;
        private readonly TimeSpan _tmpCleanupAfter;
        private readonly TimeSpan _archiveRetention;
        private readonly ILogger<FileConfirmedResultOutbox> _logger;

        public FileConfirmedResultOutbox(IConfiguration configuration, ILogger<FileConfirmedResultOutbox> logger)
        {
            _logger = logger;

            var configured = configuration["Outbox:Path"];
            if (string.IsNullOrWhiteSpace(configured))
                configured = "outbox";

            // Ruta relativa contra AppContext.BaseDirectory, no contra el CWD: un Windows
            // Service puede arrancar con CWD = C:\Windows\System32, y depender de que
            // Program.cs lo haya corregido antes acopla este dato crítico a ese orden de
            // inicialización. La carpeta del ejecutable es estable y no depende de nadie.
            _directory = Path.IsPathRooted(configured)
                ? configured
                : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, configured));
            _expiredDirectory = Path.Combine(_directory, ExpiredFolder);
            _quarantineDirectory = Path.Combine(_directory, QuarantineFolder);

            // 10 min para un .tmp: SaveAsync escribe unos pocos KB y hace el Move en
            // milisegundos, así que un .tmp de esa edad es una escritura que ya no va a
            // terminar (crash o kill) — nunca uno que un worker esté escribiendo ahora.
            _tmpCleanupAfter = TimeSpan.FromMinutes(Math.Max(1, configuration.GetValue<int>("Outbox:TmpCleanupAfterMinutes", 10)));
            _archiveRetention = TimeSpan.FromDays(Math.Max(1, configuration.GetValue<int>("Outbox:ArchiveRetentionDays", 7)));

            // Crear la carpeta acá, al construir: si no hay permisos de escritura, mejor
            // enterarse al arrancar el servicio que en medio de una caída de SQL.
            Directory.CreateDirectory(_directory);
            _logger.LogInformation("Outbox de resultados confirmados en {OutboxPath}", _directory);
        }

        public async Task SaveAsync(PendingIniResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            var finalPath = GetFilePath(result.LastInteractionId);
            var tempPath = finalPath + TempExtension;
            var json = JsonConvert.SerializeObject(result, Formatting.Indented);
            var bytes = Encoding.UTF8.GetBytes(json);

            // Escritura atómica: primero a un .tmp, Flush(true) para forzar el volcado al
            // disco (no solo al buffer del SO) y recién ahí el Move sobre el definitivo.
            // Un corte de luz o un kill a mitad de camino deja, a lo sumo, un .tmp
            // huérfano — nunca un .json truncado que el replay no pueda leer.
            await using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true))
            {
                await stream.WriteAsync(bytes);
                stream.Flush(flushToDisk: true);
            }

            File.Move(tempPath, finalPath, overwrite: true);
        }

        public Task RemoveAsync(string lastInteractionId)
        {
            var path = GetFilePath(lastInteractionId);
            if (File.Exists(path))
                File.Delete(path);
            return Task.CompletedTask;
        }

        public async Task<IReadOnlyList<PendingIniResult>> ListPendingAsync()
        {
            var pending = new List<PendingIniResult>();

            // Solo *.json de la raíz (TopDirectoryOnly): expired/ y quarantine/ quedan
            // fuera del replay. Los .tmp sobrantes son escrituras que nunca llegaron al
            // Move, es decir, resultados cuyo SaveAsync no terminó — no son confiables.
            foreach (var file in Directory.EnumerateFiles(_directory, "*" + FileExtension))
            {
                if (!IsRootJson(file))
                    continue;

                var (item, error) = await TryReadAsync(file);
                if (item is null)
                {
                    // Sin log de error acá a propósito: la cuarentena (y su log con el
                    // LastInteractionId) tiene un único dueño, RunMaintenanceAsync, que corre
                    // antes del replay en cada ciclo. Un IOException suele ser un Move
                    // concurrente de un worker; el próximo ciclo lo vuelve a leer.
                    _logger.LogDebug(error, "Archivo de outbox no legible en este ciclo, se omite del replay: {OutboxFile}", Path.GetFileName(file));
                    continue;
                }

                // Archivos sin CreatedAtUtc (default) toman la fecha del archivo: sin esto
                // parecerían tener 2000 años y el replay los expiraría al primer fallo.
                if (item.CreatedAtUtc == default)
                {
                    try { item.CreatedAtUtc = File.GetLastWriteTimeUtc(file); }
                    catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { item.CreatedAtUtc = DateTime.UtcNow; }
                }

                pending.Add(item);
            }

            return pending.OrderBy(p => p.CreatedAtUtc).ToList();
        }

        public Task<string> MoveToExpiredAsync(string lastInteractionId)
        {
            var source = GetFilePath(lastInteractionId);
            Directory.CreateDirectory(_expiredDirectory);
            var destination = MoveWithoutOverwrite(source, _expiredDirectory);
            return Task.FromResult(destination);
        }

        public async Task<OutboxMaintenanceResult> RunMaintenanceAsync()
        {
            var result = new OutboxMaintenanceResult();
            var now = DateTime.UtcNow;

            DeleteStaleTempFiles(result, now);
            await QuarantineUnreadableFilesAsync(result);
            PurgeArchive(_expiredDirectory, result, now);
            PurgeArchive(_quarantineDirectory, result, now);

            return result;
        }

        private void DeleteStaleTempFiles(OutboxMaintenanceResult result, DateTime now)
        {
            foreach (var file in SafeEnumerate(_directory, "*" + FileExtension + TempExtension, result))
            {
                if (!file.EndsWith(FileExtension + TempExtension, StringComparison.OrdinalIgnoreCase))
                    continue;

                try
                {
                    if (now - File.GetLastWriteTimeUtc(file) < _tmpCleanupAfter)
                        continue;

                    File.Delete(file);
                    result.Entries.Add(Entry(file, OutboxMaintenanceAction.TempDeleted, stemExtensions: 2));
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    // Bloqueado por un SaveAsync en curso o sin permisos: se reintenta solo
                    // en el próximo ciclo, sin afectar al resto.
                    result.Entries.Add(Entry(file, OutboxMaintenanceAction.Failed, stemExtensions: 2, detail: "borrar .tmp abandonado", error: ex));
                }
            }
        }

        private async Task QuarantineUnreadableFilesAsync(OutboxMaintenanceResult result)
        {
            foreach (var file in SafeEnumerate(_directory, "*" + FileExtension, result))
            {
                if (!IsRootJson(file))
                    continue;

                var (item, error) = await TryReadAsync(file);
                if (item is not null)
                    continue; // Válido: nunca se toca acá, es trabajo del replay.

                // Solo un contenido inválido es "corrupto". Un IOException (archivo tomado
                // por el Move de un worker o borrado entre el Enumerate y la lectura) no
                // dice nada del contenido: mandarlo a cuarentena sacaría del replay un
                // resultado válido, y con eso la fila podría reenviarse.
                if (error is not JsonException)
                {
                    if (File.Exists(file))
                        result.Entries.Add(Entry(file, OutboxMaintenanceAction.Failed, stemExtensions: 1, detail: "leer .json para validar", error: error));
                    continue;
                }

                try
                {
                    Directory.CreateDirectory(_quarantineDirectory);
                    var destination = MoveWithoutOverwrite(file, _quarantineDirectory);
                    result.Entries.Add(Entry(file, OutboxMaintenanceAction.Quarantined, stemExtensions: 1, detail: destination, error: error));
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    result.Entries.Add(Entry(file, OutboxMaintenanceAction.Failed, stemExtensions: 1, detail: "mover a quarantine", error: ex));
                }
            }
        }

        private void PurgeArchive(string directory, OutboxMaintenanceResult result, DateTime now)
        {
            if (!Directory.Exists(directory))
                return;

            foreach (var file in SafeEnumerate(directory, "*", result))
            {
                try
                {
                    // LastWriteTimeUtc se fija al momento de archivar (ver
                    // MoveWithoutOverwrite), así la retención cuenta desde que entró a la
                    // carpeta y no desde que se escribió el resultado original.
                    if (now - File.GetLastWriteTimeUtc(file) < _archiveRetention)
                        continue;

                    File.Delete(file);
                    result.Entries.Add(Entry(file, OutboxMaintenanceAction.ArchiveDeleted, stemExtensions: 1));
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    result.Entries.Add(Entry(file, OutboxMaintenanceAction.Failed, stemExtensions: 1, detail: "purgar archivo vencido de " + Path.GetFileName(directory), error: ex));
                }
            }
        }

        // Enumera a lista para que un error de la carpeta entera (permisos, ruta caída)
        // quede registrado en el resultado en vez de cortar las otras etapas.
        private static List<string> SafeEnumerate(string directory, string pattern, OutboxMaintenanceResult result)
        {
            try
            {
                return Directory.EnumerateFiles(directory, pattern).ToList();
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                result.Entries.Add(new OutboxMaintenanceEntry
                {
                    FileName = directory,
                    Action = OutboxMaintenanceAction.Failed,
                    Detail = "enumerar carpeta del outbox",
                    Error = ex
                });
                return new List<string>();
            }
        }

        private static async Task<(PendingIniResult? Item, Exception? Error)> TryReadAsync(string file)
        {
            try
            {
                var json = await File.ReadAllTextAsync(file, Encoding.UTF8);
                var item = JsonConvert.DeserializeObject<PendingIniResult>(json);
                if (item is null || string.IsNullOrWhiteSpace(item.LastInteractionId))
                    return (null, new JsonException("Contenido vacío o sin LastInteractionId"));
                return (item, null);
            }
            catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
            {
                return (null, ex);
            }
        }

        // Mueve sin pisar: si ya existe un archivo con ese nombre en destino (el mismo
        // LastInteractionId archivado antes), se agrega un sufijo de fecha. Pisarlo
        // borraría la evidencia previa que puede hacer falta para la corrección manual.
        private static string MoveWithoutOverwrite(string source, string targetDirectory)
        {
            var name = Path.GetFileName(source);
            var destination = Path.Combine(targetDirectory, name);
            if (File.Exists(destination))
            {
                var stem = Path.GetFileNameWithoutExtension(name);
                destination = Path.Combine(targetDirectory, $"{stem}_{DateTime.UtcNow:yyyyMMddHHmmssfff}{Path.GetExtension(name)}");
            }

            File.Move(source, destination, overwrite: false);

            // File.Move conserva la fecha de escritura original; se reinicia para que la
            // retención de la carpeta archivada cuente desde ahora. Si falla no importa:
            // en el peor caso el archivo se purga antes, pero el movimiento ya ocurrió.
            try { File.SetLastWriteTimeUtc(destination, DateTime.UtcNow); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }

            return destination;
        }

        private static bool IsRootJson(string file) =>
            string.Equals(Path.GetExtension(file), FileExtension, StringComparison.OrdinalIgnoreCase);

        // stemExtensions: cuántas extensiones sacar para llegar al LastInteractionId
        // (2 para "X.json.tmp", 1 para "X.json").
        private static OutboxMaintenanceEntry Entry(string file, OutboxMaintenanceAction action, int stemExtensions, string? detail = null, Exception? error = null)
        {
            var name = Path.GetFileName(file);
            var stem = name;
            for (var i = 0; i < stemExtensions; i++)
                stem = Path.GetFileNameWithoutExtension(stem);

            return new OutboxMaintenanceEntry
            {
                LastInteractionId = stem,
                FileName = name,
                Action = action,
                Detail = detail,
                Error = error
            };
        }

        private string GetFilePath(string lastInteractionId)
        {
            if (string.IsNullOrWhiteSpace(lastInteractionId))
                throw new ArgumentException("LastInteractionId vacío — no se puede ubicar el archivo de outbox.", nameof(lastInteractionId));

            return Path.Combine(_directory, Sanitize(lastInteractionId) + FileExtension);
        }

        // Los LastInteractionId son hex tipo GUID, pero se sanea igual por defensa: un
        // carácter inválido o un separador de ruta no debe poder sacar el archivo de
        // la carpeta del outbox ni hacer fallar la escritura.
        private static string Sanitize(string id)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var sb = new StringBuilder(id.Length);
            foreach (var c in id.Trim())
                sb.Append(Array.IndexOf(invalid, c) >= 0 ? '_' : c);
            return sb.ToString();
        }
    }
}
