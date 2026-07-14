namespace PacificoSeguros.Process.Services
{
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
}
