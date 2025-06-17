using System;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace RC_Proxy.Services
{
    public class RcProxyService : BackgroundService
    {
        private readonly RcProxyConfig _config;
        private readonly IRabbitMqService _rabbitMqService;
        private readonly IRewindHandler _rewindHandler;
        private readonly ILogger<RcProxyService> _logger;
        
        private TcpListener? _tcpListener;
        private TcpClient? _rcClient;
        private NetworkStream? _rcStream;
        
        private readonly ConcurrentDictionary<string, ClientConnection> _clients = new();
        private readonly CancellationTokenSource _cancellationTokenSource = new();
        
        private bool _isConnectedToRc = false;
        private uint _lastRcSequenceNumber = 0;
        private string _currentSessionId = "";

        public RcProxyService(
            IOptions<RcProxyConfig> config,
            IRabbitMqService rabbitMqService,
            IRewindHandler rewindHandler,
            ILogger<RcProxyService> logger)
        {
            _config = config.Value;
            _rabbitMqService = rabbitMqService;
            _rewindHandler = rewindHandler;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("RC_Proxy service starting...");

            try
            {
                // Initialize RabbitMQ
                if (!await _rabbitMqService.InitializeAsync())
                {
                    _logger.LogCritical("Failed to initialize RabbitMQ");
                    return;
                }

                // Start TCP listener for clients
                await StartTcpListenerAsync(stoppingToken);

                // Connect to RC server
                _ = Task.Run(() => MaintainRcConnectionAsync(stoppingToken), stoppingToken);

                // Keep service running
                await Task.Delay(-1, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("RC_Proxy service stopping...");
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "RC_Proxy service failed");
                throw;
            }
        }

        private async Task StartTcpListenerAsync(CancellationToken cancellationToken)
        {
            var localEndpoint = new IPEndPoint(IPAddress.Parse(_config.ProxyListenHost), _config.ProxyListenPort);
            _tcpListener = new TcpListener(localEndpoint);
            _tcpListener.Start();

            _logger.LogInformation("TCP listener started on {Endpoint}", localEndpoint);

            _ = Task.Run(async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var tcpClient = await _tcpListener.AcceptTcpClientAsync();
                        _ = Task.Run(() => HandleClientConnectionAsync(tcpClient, cancellationToken), cancellationToken);
                    }
                    catch (ObjectDisposedException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error accepting client connection");
                    }
                }
            }, cancellationToken);
        }

        private async Task HandleClientConnectionAsync(TcpClient tcpClient, CancellationToken cancellationToken)
        {
            var clientId = Guid.NewGuid().ToString();
            var remoteEndpoint = tcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";
            
            _logger.LogInformation("Client {ClientId} connected from {RemoteEndpoint}", clientId, remoteEndpoint);

            var clientConnection = new ClientConnection
            {
                ClientId = clientId,
                TcpClient = tcpClient,
                Stream = tcpClient.GetStream(),
                ConnectedTime = DateTime.UtcNow,
                RemoteEndpoint = remoteEndpoint
            };

            _clients.TryAdd(clientId, clientConnection);

            try
            {
                await ProcessClientMessagesAsync(clientConnection, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing client {ClientId} messages", clientId);
            }
            finally
            {
                _clients.TryRemove(clientId, out _);
                tcpClient.Close();
                _logger.LogInformation("Client {ClientId} disconnected", clientId);
            }
        }

        private async Task ProcessClientMessagesAsync(ClientConnection client, CancellationToken cancellationToken)
        {
            var buffer = new byte[4096];
            var messageBuffer = new List<byte>();

            while (!cancellationToken.IsCancellationRequested && client.TcpClient.Connected)
            {
                try
                {
                    var bytesRead = await client.Stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                    if (bytesRead == 0) break;

                    messageBuffer.AddRange(buffer.Take(bytesRead));

                    // Process complete messages
                    while (TryExtractMessage(messageBuffer, out var messageBytes))
                    {
                        await ProcessClientMessage(client, messageBytes);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading from client {ClientId}", client.ClientId);
                    break;
                }
            }
        }

        private async Task ProcessClientMessage(ClientConnection client, byte[] messageBytes)
        {
            try
            {
                var rcMessage = ParseRcMessage(messageBytes);
                rcMessage.ReceivedTime = DateTime.UtcNow;

                _logger.LogDebug("Received message from client {ClientId}: {MessageType}, SeqNum: {SeqNum}", 
                    client.ClientId, GetMessageType(rcMessage), rcMessage.Header.SequenceNumber);

                // Check if this is a rewind request
                if (RewindHandler.IsRewindRequest(rcMessage))
                {
                    var rewindRequest = RewindHandler.ParseRewindRequest(rcMessage);
                    rewindRequest.ClientId = client.ClientId;
                    
                    _logger.LogInformation("Processing rewind request from client {ClientId}, from sequence {FromSeq}",
                        client.ClientId, rewindRequest.LastSeenSequenceNumber);
                    
                    // Handle rewind from RabbitMQ instead of forwarding to RC
                    await _rewindHandler.HandleRewindRequestAsync(rewindRequest, client.Stream);
                    return;
                }

                // For other messages, forward to RC server
                if (_isConnectedToRc && _rcStream != null)
                {
                    await _rcStream.WriteAsync(messageBytes);
                    await _rcStream.FlushAsync();
                    
                    _logger.LogDebug("Forwarded message to RC server from client {ClientId}", client.ClientId);
                }
                else
                {
                    _logger.LogWarning("Cannot forward message to RC - not connected");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message from client {ClientId}", client.ClientId);
            }
        }

        private async Task MaintainRcConnectionAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (!_isConnectedToRc)
                    {
                        await ConnectToRcServerAsync();
                    }

                    if (_isConnectedToRc && _rcStream != null)
                    {
                        await ProcessRcMessagesAsync(cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in RC connection");
                    _isConnectedToRc = false;
                    
                    _rcStream?.Close();
                    _rcClient?.Close();
                    _rcStream = null;
                    _rcClient = null;
                }

                if (!_isConnectedToRc)
                {
                    _logger.LogInformation("Retrying RC connection in {Delay}ms", _config.ReconnectDelayMs);
                    await Task.Delay(_config.ReconnectDelayMs, cancellationToken);
                }
            }
        }

        private async Task ConnectToRcServerAsync()
        {
            try
            {
                _logger.LogInformation("Connecting to RC server at {Host}:{Port}", 
                    _config.RcServerHost, _config.RcServerPort);

                _rcClient = new TcpClient();
                await _rcClient.ConnectAsync(_config.RcServerHost, _config.RcServerPort);
                _rcStream = _rcClient.GetStream();
                _isConnectedToRc = true;

                _logger.LogInformation("Connected to RC server successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to connect to RC server");
                _isConnectedToRc = false;
                throw;
            }
        }

        private async Task ProcessRcMessagesAsync(CancellationToken cancellationToken)
        {
            var buffer = new byte[4096];
            var messageBuffer = new List<byte>();

            while (!cancellationToken.IsCancellationRequested && _isConnectedToRc && _rcStream != null)
            {
                try
                {
                    var bytesRead = await _rcStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                    if (bytesRead == 0) break;

                    messageBuffer.AddRange(buffer.Take(bytesRead));

                    // Process complete messages
                    while (TryExtractMessage(messageBuffer, out var messageBytes))
                    {
                        await ProcessRcMessage(messageBytes);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading from RC server");
                    _isConnectedToRc = false;
                    break;
                }
            }
        }

        private async Task ProcessRcMessage(byte[] messageBytes)
        {
            try
            {
                var rcMessage = ParseRcMessage(messageBytes);
                rcMessage.ReceivedTime = DateTime.UtcNow;

                _logger.LogDebug("Received message from RC: {MessageType}, SeqNum: {SeqNum}", 
                    GetMessageType(rcMessage), rcMessage.Header.SequenceNumber);

                // Update sequence tracking
                if (rcMessage.Header.SequenceNumber > 0)
                {
                    _lastRcSequenceNumber = rcMessage.Header.SequenceNumber;
                    _currentSessionId = rcMessage.Header.Session;
                }

                // Store CCG messages in RabbitMQ
                if (RewindHandler.IsCcgMessage(rcMessage))
                {
                    try
                    {
                        var storedMessage = RewindHandler.ExtractCcgMessage(rcMessage);
                        await _rabbitMqService.StoreCcgMessageAsync(storedMessage);
                        
                        _logger.LogDebug("Stored CCG message with sequence {SeqNum} in RabbitMQ", 
                            rcMessage.Header.SequenceNumber);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to store CCG message in RabbitMQ");
                    }
                }

                // Forward message to all connected clients
                await ForwardMessageToClients(messageBytes);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing RC message");
            }
        }

        private async Task ForwardMessageToClients(byte[] messageBytes)
        {
            var disconnectedClients = new List<string>();

            foreach (var client in _clients.Values)
            {
                try
                {
                    if (client.TcpClient.Connected)
                    {
                        await client.Stream.WriteAsync(messageBytes);
                        await client.Stream.FlushAsync();
                    }
                    else
                    {
                        disconnectedClients.Add(client.ClientId);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to forward message to client {ClientId}", client.ClientId);
                    disconnectedClients.Add(client.ClientId);
                }
            }

            // Remove disconnected clients
            foreach (var clientId in disconnectedClients)
            {
                _clients.TryRemove(clientId, out _);
            }
        }

        private static bool TryExtractMessage(List<byte> buffer, out byte[] messageBytes)
        {
            messageBytes = Array.Empty<byte>();

            if (buffer.Count < 16) return false;

            // Parse header to get message length
            var header = RcHeader.FromBytes(buffer.Take(16).ToArray());
            
            // Calculate total message length (header + blocks)
            int totalLength = 16; // Header length
            int offset = 16;
            
            for (int i = 0; i < header.BlockCount && offset < buffer.Count; i++)
            {
                if (offset + 2 > buffer.Count) return false;
                
                ushort blockLength = BitConverter.ToUInt16(buffer.Skip(offset).Take(2).ToArray(), 0);
                totalLength += 2 + blockLength; // Block header + payload
                offset += 2 + blockLength;
            }

            if (buffer.Count < totalLength) return false;

            messageBytes = buffer.Take(totalLength).ToArray();
            buffer.RemoveRange(0, totalLength);
            return true;
        }

        private static RcMessage ParseRcMessage(byte[] data)
        {
            if (data.Length < 16)
                throw new ArgumentException("Invalid message length");

            var message = new RcMessage
            {
                Header = RcHeader.FromBytes(data),
                RawData = data
            };

            int offset = 16;
            for (int i = 0; i < message.Header.BlockCount && offset < data.Length; i++)
            {
                var block = RcBlock.FromBytes(data, offset);
                message.Blocks.Add(block);
                offset += 2 + block.Length;
            }

            return message;
        }

        private static string GetMessageType(RcMessage message)
        {
            if (message.Blocks.Count == 0) return "Heartbeat";
            
            var firstBlock = message.Blocks[0];
            if (firstBlock.Payload.Length == 0) return "Empty";
            
            char messageType = (char)firstBlock.Payload[0];
            return messageType switch
            {
                'B' => "CCG",
                'R' => "Rewind",
                'r' => "RewindComplete",
                'S' => "SetControl",
                'G' => "GetControlsHistory",
                's' => "Shutdown",
                'P' => "Position",
                'C' => "Capital",
                'D' => "Debug",
                'I' => "Info",
                'W' => "Warning",
                'E' => "Error",
                _ => $"Unknown({messageType})"
            };
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping RC_Proxy service...");

            _cancellationTokenSource.Cancel();
            
            // Close all client connections
            foreach (var client in _clients.Values)
            {
                try
                {
                    client.TcpClient.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error closing client connection");
                }
            }

            // Close RC connection
            try
            {
                _rcStream?.Close();
                _rcClient?.Close();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error closing RC connection");
            }

            // Stop TCP listener
            try
            {
                _tcpListener?.Stop();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error stopping TCP listener");
            }

            await base.StopAsync(cancellationToken);
        }
    }

    // Helper class for tracking client connections
    public class ClientConnection
    {
        public string ClientId { get; set; } = "";
        public TcpClient TcpClient { get; set; } = null!;
        public NetworkStream Stream { get; set; } = null!;
        public DateTime ConnectedTime { get; set; }
        public string RemoteEndpoint { get; set; } = "";
        public bool IsAuthenticated { get; set; } = false;
        public uint LastSentSequenceNumber { get; set; } = 0;
    }
}