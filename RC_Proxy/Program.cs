using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RC_Proxy.Services;
using RC_Proxy.Configuration;
using Microsoft.Extensions.Configuration;

namespace RC_Proxy
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            
            var logger = host.Services.GetRequiredService<ILogger<Program>>();
            logger.LogInformation("Starting RC_Proxy Server...");
            
            try
            {
                await host.RunAsync();
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "RC_Proxy Server failed to start or crashed");
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                    config.AddJsonFile($"appsettings.{context.HostingEnvironment.EnvironmentName}.json", 
                                     optional: true, reloadOnChange: true);
                    config.AddEnvironmentVariables();
                    config.AddCommandLine(args);
                })
                .ConfigureServices((context, services) =>
                {
                    // Configuration
                    services.Configure<ProxyConfiguration>(
                        context.Configuration.GetSection("ProxyConfiguration"));
                    services.Configure<RabbitMqConfiguration>(
                        context.Configuration.GetSection("RabbitMq"));
                    
                    // Services
                    services.AddSingleton<IRcMessageProcessor, RcMessageProcessor>();
                    services.AddSingleton<IRabbitMqService, RabbitMqService>();
                    services.AddSingleton<ITcpProxyService, TcpProxyService>();
                    services.AddSingleton<IConnectionManager, ConnectionManager>();
                    
                    // Background Services
                    services.AddHostedService<ProxyServerHostedService>();
                    services.AddHostedService<RabbitMqPublisherService>();
                })
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                    logging.AddDebug();
                    logging.SetMinimumLevel(LogLevel.Information);
                });
    }
}