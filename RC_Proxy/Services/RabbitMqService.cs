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

            const int MAX_RETRIES = 3;
            var attempt = 0;
            
            while (attempt < MAX_RETRIES)
            {
                try
                {
                    // Walidacja wejściowej wiadomości
                    if (message.SequenceNumber == 0)
                    {
                        _logger.LogWarning("Attempting to store message with SequenceNumber 0");
                    }
                    
                    // Serializacja z walidacją
                    var messageBody = message.ToBytes();
                    _logger.LogTrace("Serialized message {SeqNum} to {Size} bytes", 
                        message.SequenceNumber, messageBody.Length);
                    
                    // Waliduj ponownie po serializacji
                    var testDeserialize = StoredCcgMessage.FromBytes(messageBody);
                    if (testDeserialize.SequenceNumber != message.SequenceNumber)
                    {
                        throw new InvalidOperationException($"Serialization validation failed: expected SeqNum {message.SequenceNumber}, got {testDeserialize.SequenceNumber}");
                    }
                    
                    var properties = _channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                    properties.MessageId = message.SequenceNumber.ToString();
                    properties.ContentType = "application/json";
                    properties.ContentEncoding = "UTF-8";
                    
                    // Dodatkowe właściwości dla diagnostyki
                    properties.Headers = new Dictionary<string, object>
                    {
                        ["OriginalSize"] = messageBody.Length,
                        ["MessageType"] = message.MessageType,
                        ["StoredAt"] = message.StoredTime.ToString("O")
                    };

                    lock (_lock)
                    {
                        _channel.BasicPublish(
                            exchange: _config.CcgMessagesExchange,
                            routingKey: _config.CcgMessagesRoutingKey,
                            basicProperties: properties,
                            body: messageBody);
                    }

                    // Weryfikacja przez natychmiastowy odczyt (opcjonalne dla krytycznych aplikacji)
                    if (_logger.IsEnabled(LogLevel.Trace))
                    {
                        try
                        {
                            await Task.Delay(10); // Krótkie opóźnienie
                            var verification = _channel.BasicGet(_config.CcgMessagesQueue, false);
                            if (verification != null)
                            {
                                var verifiedMessage = StoredCcgMessage.FromBytes(verification.Body.ToArray());
                                _logger.LogTrace("Verified stored message {SeqNum} successfully", verifiedMessage.SequenceNumber);
                                _channel.BasicNack(verification.DeliveryTag, false, true); // Zwróć do kolejki
                            }
                        }
                        catch (Exception verifyEx)
                        {
                            _logger.LogWarning(verifyEx, "Verification of stored message failed (non-critical)");
                        }
                    }

                    // Update cache
                    _messageCache.TryAdd(message.SequenceNumber, message);
                    if (message.SequenceNumber > _latestSequenceNumber)
                    {
                        _latestSequenceNumber = message.SequenceNumber;
                    }

                    _logger.LogDebug("Successfully stored CCG message with sequence number {SequenceNumber} ({Size} bytes)", 
                        message.SequenceNumber, messageBody.Length);
                    
                    return; // Sukces - wyjdź z pętli retry
                }
                catch (Exception ex)
                {
                    attempt++;
                    _logger.LogError(ex, "Failed to store CCG message with sequence number {SequenceNumber} (attempt {Attempt}/{MaxRetries})", 
                        message.SequenceNumber, attempt, MAX_RETRIES);
                    
                    if (attempt >= MAX_RETRIES)
                    {
                        _logger.LogCritical("Failed to store CCG message after {MaxRetries} attempts, giving up", MAX_RETRIES);
                        throw new InvalidOperationException($"Failed to store message after {MAX_RETRIES} attempts: {ex.Message}", ex);
                    }
                    
                    // Exponential backoff
                    await Task.Delay(TimeSpan.FromMilliseconds(100 * Math.Pow(2, attempt - 1)));
                }
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
            
            if (_channel == null) 
            {
                _logger.LogWarning("Cannot read messages - channel is null");
                return messages;
            }

            try
            {
                _logger.LogDebug("Reading messages from queue {Queue}, fromSeq: {FromSeq}, limit: {Limit}", 
                    _config.CcgMessagesQueue, fromSequenceNumber, limit);
                
                var processedCount = 0;
                var skippedCount = 0;
                var errorCount = 0;
                var maxAttempts = limit * 3; // Zabezpieczenie przed nieskończoną pętlą
                
                for (int attempt = 0; attempt < maxAttempts && processedCount < limit; attempt++)
                {
                    var result = _channel.BasicGet(_config.CcgMessagesQueue, false);
                    
                    if (result == null)
                    {
                        _logger.LogDebug("No more messages in queue after {Attempts} attempts", attempt);
                        break;
                    }
                    
                    try
                    {
                        if (result.Body.Length == 0)
                        {
                            _logger.LogWarning("Received empty message body, acknowledging and skipping");
                            _channel.BasicAck(result.DeliveryTag, false);
                            skippedCount++;
                            continue;
                        }
                        
                        // Loguj rozmiar wiadomości
                        _logger.LogTrace("Processing message: delivery tag {DeliveryTag}, size {Size} bytes", 
                            result.DeliveryTag, result.Body.Length);
                        
                        // Sprawdź czy wiadomość nie jest obcięta
                        var bodyArray = result.Body.ToArray();
                        var jsonString = System.Text.Encoding.UTF8.GetString(bodyArray);
                        
                        if (!jsonString.TrimEnd().EndsWith("}"))
                        {
                            _logger.LogError("Message appears truncated - doesn't end with }}: {JsonPreview}...", 
                                jsonString.Length > 100 ? jsonString[..100] : jsonString);
                            _channel.BasicNack(result.DeliveryTag, false, false); // Usuń uszkodzoną wiadomość
                            errorCount++;
                            continue;
                        }
                        
                        var message = StoredCcgMessage.FromBytes(bodyArray);
                        
                        if (message.SequenceNumber >= fromSequenceNumber)
                        {
                            messages.Add(message);
                            processedCount++;
                        }
                        else
                        {
                            skippedCount++;
                        }
                        
                        _channel.BasicAck(result.DeliveryTag, false);
                    }
                    catch (Exception ex)
                    {
                        errorCount++;
                        _logger.LogError(ex, "Failed to process message with delivery tag {DeliveryTag}, size {Size}. Error count: {ErrorCount}", 
                            result.DeliveryTag, result.Body.Length, errorCount);
                        
                        // Log raw data for debugging
                        if (_logger.IsEnabled(LogLevel.Debug))
                        {
                            try
                            {
                                var rawString = System.Text.Encoding.UTF8.GetString(result.Body.ToArray());
                                var preview = rawString.Length > 200 ? rawString[..200] + "..." : rawString;
                                _logger.LogDebug("Raw message data: {RawData}", preview.Replace("\n", "\\n").Replace("\r", "\\r"));
                            }
                            catch
                            {
                                _logger.LogDebug("Could not decode raw message data as UTF-8");
                            }
                        }
                        
                        _channel.BasicNack(result.DeliveryTag, false, false); // Usuń uszkodzoną wiadomość
                        
                        // Jeśli zbyt wiele błędów, przerwij
                        if (errorCount > limit / 2)
                        {
                            _logger.LogError("Too many errors ({ErrorCount}) while reading messages, stopping", errorCount);
                            break;
                        }
                    }
                }

                messages = messages.OrderBy(m => m.SequenceNumber).ToList();
                
                _logger.LogInformation("Read {ProcessedCount} messages from queue (skipped: {SkippedCount}, errors: {ErrorCount})", 
                    processedCount, skippedCount, errorCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Critical error while reading messages from queue");
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