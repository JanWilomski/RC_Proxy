// RC_Proxy/Services/HealthCheckService.cs
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RC_Proxy.Configuration;
using System.Net.NetworkInformation;

namespace RC_Proxy.Services
{
    public interface IHealthCheckService
    {
        Task<HealthStatus> GetHealthStatusAsync();
    }

    public class HealthCheckService : IHealthCheckService
    {
        private readonly ILogger<HealthCheckService> _logger;
        private readonly ProxyConfiguration _proxyConfig;
        private readonly RabbitMqConfiguration _rabbitConfig;
        private readonly IConnectionManager _connectionManager;
        private readonly ITcpProxyService _proxyService;

        public HealthCheckService(
            ILogger<HealthCheckService> logger,
            IOptions<ProxyConfiguration> proxyConfig,
            IOptions<RabbitMqConfiguration> rabbitConfig,
            IConnectionManager connectionManager,
            ITcpProxyService proxyService)
        {
            _logger = logger;
            _proxyConfig = proxyConfig.Value;
            _rabbitConfig = rabbitConfig.Value;
            _connectionManager = connectionManager;
            _proxyService = proxyService;
        }

        public async Task<HealthStatus> GetHealthStatusAsync()
        {
            var status = new HealthStatus
            {
                Timestamp = DateTime.UtcNow,
                ProxyServiceRunning = _proxyService.IsRunning,
                ActiveConnections = _connectionManager.ActiveConnectionCount
            };

            // Check RC server connectivity
            status.RcServerReachable = await CheckRcServerConnectivity();

            // Check RabbitMQ connectivity
            status.RabbitMqReachable = await CheckRabbitMqConnectivity();

            // Overall health
            status.IsHealthy = status.ProxyServiceRunning && 
                              status.RcServerReachable && 
                              status.RabbitMqReachable;

            return status;
        }

        private async Task<bool> CheckRcServerConnectivity()
        {
            try
            {
                using var ping = new Ping();
                var reply = await ping.SendPingAsync(_proxyConfig.RcServerHost, 3000);
                return reply.Status == IPStatus.Success;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to ping RC server");
                return false;
            }
        }

        private async Task<bool> CheckRabbitMqConnectivity()
        {
            try
            {
                using var ping = new Ping();
                var reply = await ping.SendPingAsync(_rabbitConfig.HostName, 3000);
                return reply.Status == IPStatus.Success;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to ping RabbitMQ server");
                return false;
            }
        }
    }

    public class HealthStatus
    {
        public DateTime Timestamp { get; set; }
        public bool IsHealthy { get; set; }
        public bool ProxyServiceRunning { get; set; }
        public bool RcServerReachable { get; set; }
        public bool RabbitMqReachable { get; set; }
        public int ActiveConnections { get; set; }
        public string[] Issues { get; set; } = Array.Empty<string>();
    }
}