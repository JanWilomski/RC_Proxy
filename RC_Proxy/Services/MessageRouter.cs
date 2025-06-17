using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RC_Proxy.Services
{
    // Message router interface (for future extensions)
    public interface IMessageRouter
    {
        Task<RoutedMessage> RouteMessageAsync(RcMessage message, MessageDirection direction, string clientId);
        Task<bool> ShouldForwardToRc(RcMessage message, string clientId);
        Task<bool> ShouldForwardToClient(RcMessage message, string clientId);
    }

    // Simple message router implementation
    public class MessageRouter : IMessageRouter
    {
        private readonly ILogger<MessageRouter> _logger;

        public MessageRouter(ILogger<MessageRouter> logger)
        {
            _logger = logger;
        }

        public async Task<RoutedMessage> RouteMessageAsync(RcMessage message, MessageDirection direction, string clientId)
        {
            var routedMessage = new RoutedMessage
            {
                Direction = direction,
                ClientId = clientId,
                Message = message,
                ProcessedTime = DateTime.UtcNow
            };

            // Check if this is a rewind request
            if (RewindHandler.IsRewindRequest(message))
            {
                // Don't forward rewind requests to RC - handle them locally
                routedMessage.ShouldForwardToRc = false;
                routedMessage.ShouldForwardToClient = false; // Will be handled by RewindHandler
            }
            else
            {
                // Forward all other messages normally
                routedMessage.ShouldForwardToRc = direction == MessageDirection.ClientToRc;
                routedMessage.ShouldForwardToClient = direction == MessageDirection.RcToClient;
            }

            return routedMessage;
        }

        public async Task<bool> ShouldForwardToRc(RcMessage message, string clientId)
        {
            // Don't forward rewind requests to RC
            if (RewindHandler.IsRewindRequest(message))
            {
                _logger.LogDebug("Intercepting rewind request from client {ClientId}", clientId);
                return false;
            }

            return true;
        }

        public async Task<bool> ShouldForwardToClient(RcMessage message, string clientId)
        {
            return true;
        }
    }

    // RC Connection Manager interface
    public interface IRcConnectionManager
    {
        Task<bool> ConnectAsync();
        Task DisconnectAsync();
        bool IsConnected { get; }
        event Action<bool> ConnectionStatusChanged;
        Task<bool> SendMessageAsync(byte[] messageData);
        event Action<byte[]> MessageReceived;
    }

    // Health check service for monitoring
    public interface IHealthCheckService
    {
        Task<bool> IsHealthyAsync();
        Task<Dictionary<string, object>> GetHealthStatusAsync();
    }

    public class HealthCheckService : IHealthCheckService
    {
        private readonly IRabbitMqService _rabbitMqService;
        private readonly IRcConnectionManager _rcConnectionManager;
        private readonly ILogger<HealthCheckService> _logger;

        public HealthCheckService(
            IRabbitMqService rabbitMqService,
            IRcConnectionManager rcConnectionManager,
            ILogger<HealthCheckService> logger)
        {
            _rabbitMqService = rabbitMqService;
            _rcConnectionManager = rcConnectionManager;
            _logger = logger;
        }

        public async Task<bool> IsHealthyAsync()
        {
            try
            {
                var rabbitMqHealthy = await _rabbitMqService.IsHealthyAsync();
                var rcConnected = _rcConnectionManager.IsConnected;

                return rabbitMqHealthy && rcConnected;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking health status");
                return false;
            }
        }

        public async Task<Dictionary<string, object>> GetHealthStatusAsync()
        {
            var status = new Dictionary<string, object>();

            try
            {
                status["RabbitMQ"] = await _rabbitMqService.IsHealthyAsync();
                status["RcConnection"] = _rcConnectionManager.IsConnected;
                status["Timestamp"] = DateTime.UtcNow;
                status["OverallHealth"] = await IsHealthyAsync();
                
                // Add queue statistics if available
                try
                {
                    var latestSeqNum = await _rabbitMqService.GetLatestSequenceNumberAsync();
                    status["LatestSequenceNumber"] = latestSeqNum;
                }
                catch (Exception ex)
                {
                    status["QueueError"] = ex.Message;
                }
            }
            catch (Exception ex)
            {
                status["Error"] = ex.Message;
                status["OverallHealth"] = false;
            }

            return status;
        }
    }

    // Statistics service for monitoring
    public interface IStatisticsService
    {
        void IncrementClientConnections();
        void DecrementClientConnections();
        void IncrementMessagesForwarded(MessageDirection direction);
        void IncrementCcgMessagesStored();
        void IncrementRewindRequestsHandled();
        Task<Dictionary<string, object>> GetStatisticsAsync();
    }

    public class StatisticsService : IStatisticsService
    {
        private long _clientConnections = 0;
        private long _messagesForwardedToRc = 0;
        private long _messagesForwardedToClients = 0;
        private long _ccgMessagesStored = 0;
        private long _rewindRequestsHandled = 0;
        private readonly DateTime _startTime = DateTime.UtcNow;

        public void IncrementClientConnections()
        {
            Interlocked.Increment(ref _clientConnections);
        }

        public void DecrementClientConnections()
        {
            Interlocked.Decrement(ref _clientConnections);
        }

        public void IncrementMessagesForwarded(MessageDirection direction)
        {
            if (direction == MessageDirection.ClientToRc)
                Interlocked.Increment(ref _messagesForwardedToRc);
            else if (direction == MessageDirection.RcToClient)
                Interlocked.Increment(ref _messagesForwardedToClients);
        }

        public void IncrementCcgMessagesStored()
        {
            Interlocked.Increment(ref _ccgMessagesStored);
        }

        public void IncrementRewindRequestsHandled()
        {
            Interlocked.Increment(ref _rewindRequestsHandled);
        }

        public async Task<Dictionary<string, object>> GetStatisticsAsync()
        {
            var uptime = DateTime.UtcNow - _startTime;
            
            return new Dictionary<string, object>
            {
                ["ClientConnections"] = _clientConnections,
                ["MessagesForwardedToRc"] = _messagesForwardedToRc,
                ["MessagesForwardedToClients"] = _messagesForwardedToClients,
                ["CcgMessagesStored"] = _ccgMessagesStored,
                ["RewindRequestsHandled"] = _rewindRequestsHandled,
                ["UptimeSeconds"] = uptime.TotalSeconds,
                ["StartTime"] = _startTime,
                ["CurrentTime"] = DateTime.UtcNow
            };
        }
    }

    // Extension methods for better logging
    public static class LoggingExtensions
    {
        public static void LogMessageReceived(this ILogger logger, string source, string messageType, uint sequenceNumber, string? clientId = null)
        {
            if (clientId != null)
            {
                logger.LogDebug("Received {MessageType} message from {Source} (Client: {ClientId}, SeqNum: {SeqNum})", 
                    messageType, source, clientId, sequenceNumber);
            }
            else
            {
                logger.LogDebug("Received {MessageType} message from {Source} (SeqNum: {SeqNum})", 
                    messageType, source, sequenceNumber);
            }
        }

        public static void LogMessageForwarded(this ILogger logger, string destination, string messageType, uint sequenceNumber, string? clientId = null)
        {
            if (clientId != null)
            {
                logger.LogDebug("Forwarded {MessageType} message to {Destination} (Client: {ClientId}, SeqNum: {SeqNum})", 
                    messageType, destination, clientId, sequenceNumber);
            }
            else
            {
                logger.LogDebug("Forwarded {MessageType} message to {Destination} (SeqNum: {SeqNum})", 
                    messageType, destination, sequenceNumber);
            }
        }

        public static void LogRewindRequest(this ILogger logger, string clientId, uint fromSequence, int messageCount)
        {
            logger.LogInformation("Handling rewind request from client {ClientId}: from sequence {FromSeq}, returning {Count} messages", 
                clientId, fromSequence, messageCount);
        }

        public static void LogCcgMessageStored(this ILogger logger, uint sequenceNumber, string messageType, uint? instrumentId = null)
        {
            if (instrumentId.HasValue)
            {
                logger.LogDebug("Stored CCG message: {MessageType} (SeqNum: {SeqNum}, InstrumentId: {InstrumentId})", 
                    messageType, sequenceNumber, instrumentId);
            }
            else
            {
                logger.LogDebug("Stored CCG message: {MessageType} (SeqNum: {SeqNum})", 
                    messageType, sequenceNumber);
            }
        }
    }
}