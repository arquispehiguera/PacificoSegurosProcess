namespace PacificoSeguros.Process.Services
{
    public interface IHeartbeatMonitor
    {
        void ReportAlive();
        DateTime LastHeartbeatUtc { get; }

        void ReportProgress();
        DateTime LastProgressUtc { get; }
    }
}
