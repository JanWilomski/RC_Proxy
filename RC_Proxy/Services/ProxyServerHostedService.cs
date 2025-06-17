// RC_Proxy/Services/ProxyServerHostedService.cs
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace RC_Proxy.Services
{
    public class ProxyServerHostedService : BackgroundService
    {
        private readonly ILogger<ProxyServerHostedService> _logger;
        private readonly ITcpProxyService _proxyService;
        private readonly IRabbitMqService _rabbitMqService;

        public ProxyServerHostedService(
            ILogger<ProxyServerHostedService> logger,
            ITcpProxyService proxyService,
            IRabbitMqService rabbitMqService)
        {
            _logger = logger;
            _proxyService = proxyService;
            _rabbitMqService = rabbitMqService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                _logger.LogInformation("Starting RC_Proxy services...");

                // Initialize RabbitMQ first
                await _rabbitMqService.InitializeAsync();
                _logger.LogInformation("RabbitMQ service initialized");

                // Start TCP proxy
                await _proxyService.StartAsync(stoppingToken);
                _logger.LogInformation("TCP proxy service started");

                // Keep running until cancellation is requested
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("RC_Proxy service shutdown requested");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "RC_Proxy service failed");
                throw;
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping RC_Proxy services...");

            try
            {
                await _proxyService.StopAsync(cancellationToken);
                _rabbitMqService.Dispose();
                
                _logger.LogInformation("RC_Proxy services stopped successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error stopping RC_Proxy services");
            }

            await base.StopAsync(cancellationToken);
        }
    }

    // RC_Proxy/Services/RabbitMqPublisherService.cs
    public class RabbitMqPublisherService : BackgroundService
    {
        private readonly ILogger<RabbitMqPublisherService> _logger;
        private readonly IConnectionManager _connectionManager;
        private readonly IRabbitMqService _rabbitMqService;

        public RabbitMqPublisherService(
            ILogger<RabbitMqPublisherService> logger,
            IConnectionManager connectionManager,
            IRabbitMqService rabbitMqService)
        {
            _logger = logger;
            _connectionManager = connectionManager;
            _rabbitMqService = rabbitMqService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("RabbitMQ publisher service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Publish connection statistics every 30 seconds
                    await PublishConnectionStats();
                    await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in RabbitMQ publisher service");
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }

            _logger.LogInformation("RabbitMQ publisher service stopped");
        }

        private async Task PublishConnectionStats()
        {
            var connections = _connectionManager.GetAllConnections();
            var stats = new
            {
                Timestamp = DateTime.UtcNow,
                ActiveConnections = connections.Count,
                Connections = connections.Select(c => new
                {
                    c.ConnectionId,
                    c.ConnectedTime,
                    c.BytesClientToServer,
                    c.BytesServerToClient,
                    DurationMinutes = (DateTime.UtcNow - c.ConnectedTime).TotalMinutes
                }).ToList()
            };

            _logger.LogDebug($"Connection stats: {connections.Count} active connections");
        }
    }
}