using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using RC_Proxy.Services;

namespace RC_Proxy
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            
            var logger = host.Services.GetRequiredService<ILogger<Program>>();
            logger.LogInformation("RC_Proxy starting...");
            
            try
            {
                await host.RunAsync();
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "RC_Proxy terminated unexpectedly");
                throw;
            }
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                    config.AddEnvironmentVariables();
                    config.AddCommandLine(args);
                })
                .ConfigureServices((context, services) =>
                {
                    var configuration = context.Configuration;
                    
                    // Configuration sections
                    services.Configure<RcProxyConfig>(configuration.GetSection("RcProxy"));
                    services.Configure<RabbitMqConfig>(configuration.GetSection("RabbitMQ"));
                    
                    // Services
                    services.AddSingleton<IRabbitMqService, RabbitMqService>();
                    services.AddSingleton<IMessageRouter, MessageRouter>();
                    services.AddSingleton<IRewindHandler, RewindHandler>();
                    services.AddSingleton<IRcConnectionManager, RcConnectionManager>();
                    
                    // Hosted services
                    services.AddHostedService<RcProxyService>();
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