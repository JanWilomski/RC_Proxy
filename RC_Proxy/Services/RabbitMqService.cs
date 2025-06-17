// RC_Proxy/Services/RabbitMqService.cs

using System.Net.Sockets;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RC_Proxy.Configuration;
using RC_Proxy.Models;
using System.Text;
using System.Text.Json;

namespace RC_Proxy.Services
{
    public interface IRabbitMqService
    {
        Task InitializeAsync();
        Task PublishRcMessageAsync(RcMessage message);
        Task PublishCcgMessageAsync(CcgMessageInfo ccgMessage);
        Task<IDisposable> SubscribeToCcgMessagesAsync(Func<CcgMessageInfo, Task> onMessage);
        Task<IDisposable> SubscribeToAllMessagesAsync(Func<RcMessage, Task> onMessage);
        void Dispose();
    }

    public class RabbitMqService : IRabbitMqService, IDisposable
    {
        private readonly ILogger<RabbitMqService> _logger;
        private readonly RabbitMqConfiguration _config;
        private IConnection _connection;
        private IModel _channel;
        private bool _disposed;

        public RabbitMqService(
            ILogger<RabbitMqService> logger,
            IOptions<RabbitMqConfiguration> config)
        {
            _logger = logger;
            _config = config.Value;
        }

        public async Task InitializeAsync()
        {
            try
            {
                var factory = new ConnectionFactory()
                {
                    HostName = _config.HostName,
                    Port = _config.Port,
                    UserName = _config.UserName,
                    Password = _config.Password,
                    VirtualHost = _config.VirtualHost,
                    AutomaticRecoveryEnabled = true,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                };

                _connection = factory.CreateConnection("RC_Proxy");
                _channel = _connection.CreateModel();

                // Declare exchange
                _channel.ExchangeDeclare(
                    exchange: _config.ExchangeName,
                    type: ExchangeType.Topic,
                    durable: true,
                    autoDelete: false);

                // Declare queues
                var queueArgs = new Dictionary<string, object>();
                if (_config.EnableMessagePersistence)
                {
                    queueArgs.Add("x-message-ttl", _config.MessageTtlMinutes * 60 * 1000);
                }

                // CCG Messages queue
                _channel.QueueDeclare(
                    queue: _config.CcgQueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: queueArgs);

                _channel.QueueBind(
                    queue: _config.CcgQueueName,
                    exchange: _config.ExchangeName,
                    routingKey: "ccg.*");

                // All RC Messages queue
                _channel.QueueDeclare(
                    queue: _config.AllMessagesQueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: queueArgs);

                _channel.QueueBind(
                    queue: _config.AllMessagesQueueName,
                    exchange: _config.ExchangeName,
                    routingKey: "rc.*");

                _logger.LogInformation("RabbitMQ connection established and queues configured");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize RabbitMQ connection");
                throw;
            }
        }

        public async Task PublishRcMessageAsync(RcMessage message)
        {
            if (_disposed || _channel == null)
                return;

            try
            {
                var messageBody = JsonSerializer.Serialize(new
                {
                    message.Header,
                    BlockCount = message.Blocks.Count,
                    message.ReceivedTime,
                    message.ConnectionId,
                    message.Direction,
                    HasCcgMessages = message.Blocks.Any(b => b.IsCcgMessage),
                    RawDataLength = message.RawData.Length,
                    RawDataBase64 = Convert.ToBase64String(message.RawData)
                });

                var body = Encoding.UTF8.GetBytes(messageBody);
                var routingKey = $"rc.{message.Direction.ToString().ToLower()}.{message.ConnectionId}";

                var properties = _channel.CreateBasicProperties();
                properties.Persistent = _config.EnableMessagePersistence;
                properties.Timestamp = new AmqpTimestamp(
                    ((DateTimeOffset)message.ReceivedTime).ToUnixTimeSeconds());
                properties.Headers = new Dictionary<string, object>
                {
                    ["connection_id"] = message.ConnectionId,
                    ["direction"] = message.Direction.ToString(),
                    ["sequence_number"] = message.Header.SequenceNumber,
                    ["block_count"] = message.Blocks.Count
                };

                _channel.BasicPublish(
                    exchange: _config.ExchangeName,
                    routingKey: routingKey,
                    basicProperties: properties,
                    body: body);

                _logger.LogDebug($"Published RC message: {routingKey}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to publish RC message for connection {message.ConnectionId}");
            }
        }

        public async Task PublishCcgMessageAsync(CcgMessageInfo ccgMessage)
        {
            if (_disposed || _channel == null)
                return;

            try
            {
                var messageBody = JsonSerializer.Serialize(new
                {
                    ccgMessage.SequenceNumber,
                    ccgMessage.ReceivedTime,
                    ccgMessage.ConnectionId,
                    ccgMessage.MessageType,
                    ccgMessage.MessageName,
                    ccgMessage.InstrumentId,
                    CcgDataBase64 = Convert.ToBase64String(ccgMessage.CcgData),
                    CcgDataLength = ccgMessage.CcgData.Length
                });

                var body = Encoding.UTF8.GetBytes(messageBody);
                var routingKey = $"ccg.{ccgMessage.MessageName.ToLower()}.{ccgMessage.ConnectionId}";

                var properties = _channel.CreateBasicProperties();
                properties.Persistent = _config.EnableMessagePersistence;
                properties.Timestamp = new AmqpTimestamp(
                    ((DateTimeOffset)ccgMessage.ReceivedTime).ToUnixTimeSeconds());
                properties.Headers = new Dictionary<string, object>
                {
                    ["connection_id"] = ccgMessage.ConnectionId,
                    ["message_type"] = ccgMessage.MessageType,
                    ["message_name"] = ccgMessage.MessageName,
                    ["sequence_number"] = ccgMessage.SequenceNumber
                };

                if (ccgMessage.InstrumentId.HasValue)
                {
                    properties.Headers["instrument_id"] = ccgMessage.InstrumentId.Value;
                }

                _channel.BasicPublish(
                    exchange: _config.ExchangeName,
                    routingKey: routingKey,
                    basicProperties: properties,
                    body: body);

                _logger.LogDebug($"Published CCG message: {routingKey}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to publish CCG message for connection {ccgMessage.ConnectionId}");
            }
        }

        public async Task<IDisposable> SubscribeToCcgMessagesAsync(Func<CcgMessageInfo, Task> onMessage)
        {
            if (_disposed || _channel == null)
                throw new InvalidOperationException("RabbitMQ service not initialized");

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.Received += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var ccgInfo = JsonSerializer.Deserialize<CcgMessageInfo>(message);
                    
                    // Decode base64 CCG data
                    if (ccgInfo != null)
                    {
                        await onMessage(ccgInfo);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing CCG message from queue");
                }
            };

            var consumerTag = _channel.BasicConsume(
                queue: _config.CcgQueueName,
                autoAck: true,
                consumer: consumer);

            return new RabbitMqSubscription(_channel, consumerTag, _logger);
        }

        public async Task<IDisposable> SubscribeToAllMessagesAsync(Func<RcMessage, Task> onMessage)
        {
            if (_disposed || _channel == null)
                throw new InvalidOperationException("RabbitMQ service not initialized");

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.Received += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var rcMessage = JsonSerializer.Deserialize<RcMessage>(message);
                    
                    if (rcMessage != null)
                    {
                        await onMessage(rcMessage);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing RC message from queue");
                }
            };

            var consumerTag = _channel.BasicConsume(
                queue: _config.AllMessagesQueueName,
                autoAck: true,
                consumer: consumer);

            return new RabbitMqSubscription(_channel, consumerTag, _logger);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                _channel?.Close();
                _channel?.Dispose();
                _connection?.Close();
                _connection?.Dispose();

                _logger.LogInformation("RabbitMQ connection closed");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing RabbitMQ connection");
            }
        }
    }

    public class RabbitMqSubscription : IDisposable
    {
        private readonly IModel _channel;
        private readonly string _consumerTag;
        private readonly ILogger _logger;
        private bool _disposed;

        public RabbitMqSubscription(IModel channel, string consumerTag, ILogger logger)
        {
            _channel = channel;
            _consumerTag = consumerTag;
            _logger = logger;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                _channel?.BasicCancel(_consumerTag);
                _logger.LogDebug($"Cancelled RabbitMQ consumer: {_consumerTag}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error cancelling RabbitMQ consumer: {_consumerTag}");
            }
        }
    }

    // RC_Proxy/Services/ConnectionManager.cs
    public interface IConnectionManager
    {
        void AddConnection(ProxyConnection connection);
        void RemoveConnection(string connectionId);
        ProxyConnection GetConnection(string connectionId);
        List<ProxyConnection> GetAllConnections();
        Task CloseAllConnectionsAsync();
        int ActiveConnectionCount { get; }
    }

    public class ConnectionManager : IConnectionManager
    {
        private readonly ILogger<ConnectionManager> _logger;
        private readonly Dictionary<string, ProxyConnection> _connections = new();
        private readonly object _lock = new();

        public int ActiveConnectionCount
        {
            get
            {
                lock (_lock)
                {
                    return _connections.Count;
                }
            }
        }

        public ConnectionManager(ILogger<ConnectionManager> logger)
        {
            _logger = logger;
        }

        public void AddConnection(ProxyConnection connection)
        {
            lock (_lock)
            {
                _connections[connection.ConnectionId] = connection;
                _logger.LogInformation($"Added connection {connection.ConnectionId}. Total active: {_connections.Count}");
            }
        }

        public void RemoveConnection(string connectionId)
        {
            lock (_lock)
            {
                if (_connections.Remove(connectionId))
                {
                    _logger.LogInformation($"Removed connection {connectionId}. Total active: {_connections.Count}");
                }
            }
        }

        public ProxyConnection GetConnection(string connectionId)
        {
            lock (_lock)
            {
                _connections.TryGetValue(connectionId, out var connection);
                return connection;
            }
        }

        public List<ProxyConnection> GetAllConnections()
        {
            lock (_lock)
            {
                return new List<ProxyConnection>(_connections.Values);
            }
        }

        public async Task CloseAllConnectionsAsync()
        {
            List<ProxyConnection> connections;
            
            lock (_lock)
            {
                connections = new List<ProxyConnection>(_connections.Values);
                _connections.Clear();
            }

            foreach (var connection in connections)
            {
                try
                {
                    connection.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error closing connection {connection.ConnectionId}");
                }
            }

            _logger.LogInformation($"Closed {connections.Count} connections");
        }
    }

    // RC_Proxy/Models/ProxyConnection.cs
    public class ProxyConnection : IDisposable
    {
        public string ConnectionId { get; set; }
        public TcpClient ClientSocket { get; set; }
        public TcpClient ServerSocket { get; set; }
        public NetworkStream ClientStream { get; set; }
        public NetworkStream ServerStream { get; set; }
        public DateTime ConnectedTime { get; set; }
        public long BytesClientToServer { get; set; }
        public long BytesServerToClient { get; set; }
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                ClientStream?.Close();
                ServerStream?.Close();
                ClientSocket?.Close();
                ServerSocket?.Close();
            }
            catch
            {
                // Ignore errors during cleanup
            }
        }
    }
}