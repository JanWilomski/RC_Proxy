// RC_Proxy/Configuration/ProxyConfiguration.cs
namespace RC_Proxy.Configuration
{
    public class ProxyConfiguration
    {
        public string RcServerHost { get; set; } = "127.0.0.1";
        public int RcServerPort { get; set; } = 19083;
        public string ProxyListenHost { get; set; } = "127.0.0.1";
        public int ProxyListenPort { get; set; } = 19084;
        public int MaxConcurrentConnections { get; set; } = 100;
        public int BufferSize { get; set; } = 8192;
        public bool LogRawMessages { get; set; } = false;
        public bool EnableMessageBuffering { get; set; } = true;
        public int MessageBufferSize { get; set; } = 10000;
    }

    public class RabbitMqConfiguration
    {
        public string HostName { get; set; } = "localhost";
        public int Port { get; set; } = 5672;
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public string VirtualHost { get; set; } = "/";
        public string ExchangeName { get; set; } = "rc_messages";
        public string CcgQueueName { get; set; } = "ccg_messages";
        public string AllMessagesQueueName { get; set; } = "all_rc_messages";
        public bool EnableMessagePersistence { get; set; } = true;
        public int MessageTtlMinutes { get; set; } = 60;
    }
}