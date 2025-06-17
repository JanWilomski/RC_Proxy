using System;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RC_Proxy.Services
{
    public interface IRabbitMqService : IDisposable
    {
        Task<bool> InitializeAsync();
        Task StoreCcgMessageAsync(StoredCcgMessage message);
        Task<List<StoredCcgMessage>> GetCcgMessagesAsync(uint fromSequenceNumber = 0, int limit = 1000);
        Task<uint> GetLatestSequenceNumberAsync();
        Task<bool> IsHealthyAsync();
    }

    public class RabbitMqService : IRabbitMqService, IDisposable
    {
        private readonly RabbitMqConfig _config;
        private readonly ILogger<RabbitMqService> _logger;
        
        private IConnection? _connection;
        private IModel? _channel;
        private readonly object _lock = new object();
        private bool _disposed = false;
        
        // In-memory cache for quick lookups
        private readonly ConcurrentDictionary<uint, StoredCcgMessage> _messageCache 
            = new ConcurrentDictionary<uint, StoredCcgMessage>();
        private uint _latestSequenceNumber = 0;

        public RabbitMqService(IOptions<RabbitMqConfig> config, ILogger<RabbitMqService> logger)
        {
            _config = config.Value;
            _logger = logger;
        }

        public async Task<bool> InitializeAsync()
        {
            try
            {
                _logger.LogInformation("Initializing RabbitMQ connection to {Host}:{Port}", 
                    _config.HostName, _config.Port);

                var factory = new ConnectionFactory()
                {
                    HostName = _config.HostName,
                    Port = _config.Port,
                    UserName = _config.UserName,
                    Password = _config.Password,
                    VirtualHost = _config.VirtualHost,
                    AutomaticRecoveryEnabled = true,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
                    RequestedHeartbeat = TimeSpan.FromSeconds(60)
                };

                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();

                // Declare exchange
                _channel.ExchangeDeclare(
                    exchange: _config.CcgMessagesExchange,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false);

                // Declare queue with TTL and size limits
                var queueArgs = new Dictionary<string, object>
                {
                    ["x-message-ttl"] = _config.MessageTtlHours * 60 * 60 * 1000,
                    ["x-max-length"] = _config.MaxQueueSize,
                    ["x-overflow"] = "drop-head" // Remove oldest messages when queue is full
                };

                _channel.QueueDeclare(
                    queue: _config.CcgMessagesQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: queueArgs);

                // Bind queue to exchange
                _channel.QueueBind(
                    queue: _config.CcgMessagesQueue,
                    exchange: _config.CcgMessagesExchange,
                    routingKey: _config.CcgMessagesRoutingKey);

                _logger.LogInformation("RabbitMQ initialized successfully");

                // Load recent messages into cache
                await LoadRecentMessagesIntoCache();

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize RabbitMQ");
                return false;
            }
        }

        public async Task StoreCcgMessageAsync(StoredCcgMessage message)
        {
            if (_channel == null)
            {
                _logger.LogWarning("Cannot store message - RabbitMQ not initialized");
                return;
            }

            try
            {
                var messageBody = message.ToBytes();
                var properties = _channel.CreateBasicProperties();
                properties.Persistent = true;
                properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                properties.MessageId = message.SequenceNumber.ToString();

                lock (_lock)
                {
                    _channel.BasicPublish(
                        exchange: _config.CcgMessagesExchange,
                        routingKey: _config.CcgMessagesRoutingKey,
                        basicProperties: properties,
                        body: messageBody);
                }

                // Update cache
                _messageCache.TryAdd(message.SequenceNumber, message);
                if (message.SequenceNumber > _latestSequenceNumber)
                {
                    _latestSequenceNumber = message.SequenceNumber;
                }

                _logger.LogDebug("Stored CCG message with sequence number {SequenceNumber}", 
                    message.SequenceNumber);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to store CCG message with sequence number {SequenceNumber}", 
                    message.SequenceNumber);
                throw;
            }
        }

        public async Task<List<StoredCcgMessage>> GetCcgMessagesAsync(uint fromSequenceNumber = 0, int limit = 1000)
        {
            var messages = new List<StoredCcgMessage>();

            try
            {
                // First, try to get messages from cache
                var cachedMessages = _messageCache.Values
                    .Where(m => m.SequenceNumber >= fromSequenceNumber)
                    .OrderBy(m => m.SequenceNumber)
                    .Take(limit)
                    .ToList();

                if (cachedMessages.Count >= limit || fromSequenceNumber > _latestSequenceNumber)
                {
                    _logger.LogDebug("Retrieved {Count} messages from cache (from seq {FromSeq})", 
                        cachedMessages.Count, fromSequenceNumber);
                    return cachedMessages;
                }

                // If cache doesn't have enough messages, read from RabbitMQ
                messages = await ReadMessagesFromQueue(fromSequenceNumber, limit);
                
                _logger.LogDebug("Retrieved {Count} messages from RabbitMQ (from seq {FromSeq})", 
                    messages.Count, fromSequenceNumber);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to get CCG messages from sequence {FromSeq}", fromSequenceNumber);
                throw;
            }

            return messages;
        }

        private async Task<List<StoredCcgMessage>> ReadMessagesFromQueue(uint fromSequenceNumber, int limit)
        {
            var messages = new List<StoredCcgMessage>();
            
            if (_channel == null) return messages;

            try
            {
                // Create a temporary consumer to read messages
                var tempQueueName = _channel.QueueDeclare().QueueName;
                var consumer = new EventingBasicConsumer(_channel);
                var receivedMessages = new List<BasicDeliverEventArgs>();

                consumer.Received += (model, ea) =>
                {
                    receivedMessages.Add(ea);
                };

                // Start consuming
                var consumerTag = _channel.BasicConsume(
                    queue: _config.CcgMessagesQueue,
                    autoAck: false,
                    consumer: consumer);

                // Read messages for a short time
                await Task.Delay(1000);

                // Stop consuming
                _channel.BasicCancel(consumerTag);

                // Process received messages
                foreach (var ea in receivedMessages)
                {
                    try
                    {
                        var message = StoredCcgMessage.FromBytes(ea.Body.ToArray());
                        if (message.SequenceNumber >= fromSequenceNumber)
                        {
                            messages.Add(message);
                        }
                        
                        // Acknowledge the message
                        _channel.BasicAck(ea.DeliveryTag, false);
                        
                        if (messages.Count >= limit) break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to deserialize message");
                        _channel.BasicNack(ea.DeliveryTag, false, false);
                    }
                }

                messages = messages.OrderBy(m => m.SequenceNumber).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read messages from queue");
                throw;
            }

            return messages;
        }

        public async Task<uint> GetLatestSequenceNumberAsync()
        {
            return _latestSequenceNumber;
        }

        private async Task LoadRecentMessagesIntoCache()
        {
            try
            {
                _logger.LogInformation("Loading recent messages into cache...");
                
                // Load last 1000 messages into cache for fast access
                var recentMessages = await ReadMessagesFromQueue(0, 1000);
                
                foreach (var message in recentMessages)
                {
                    _messageCache.TryAdd(message.SequenceNumber, message);
                    if (message.SequenceNumber > _latestSequenceNumber)
                    {
                        _latestSequenceNumber = message.SequenceNumber;
                    }
                }

                _logger.LogInformation("Loaded {Count} messages into cache, latest seq: {LatestSeq}", 
                    recentMessages.Count, _latestSequenceNumber);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load recent messages into cache");
            }
        }

        public async Task<bool> IsHealthyAsync()
        {
            try
            {
                return _connection?.IsOpen == true && _channel?.IsOpen == true;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _channel?.Close();
                _channel?.Dispose();
                _connection?.Close();
                _connection?.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing RabbitMQ resources");
            }

            _disposed = true;
        }
    }
}