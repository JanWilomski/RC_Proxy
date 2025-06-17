// RC_Proxy/Services/TcpProxyService.cs
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RC_Proxy.Configuration;
using RC_Proxy.Models;

namespace RC_Proxy.Services
{
    public interface ITcpProxyService
    {
        Task StartAsync(CancellationToken cancellationToken);
        Task StopAsync(CancellationToken cancellationToken);
        bool IsRunning { get; }
    }

    public class TcpProxyService : ITcpProxyService
    {
        private readonly ILogger<TcpProxyService> _logger;
        private readonly ProxyConfiguration _config;
        private readonly IRcMessageProcessor _messageProcessor;
        private readonly IConnectionManager _connectionManager;
        private TcpListener _listener;
        private CancellationTokenSource _cancellationTokenSource;
        private bool _isRunning;

        public bool IsRunning => _isRunning;

        public TcpProxyService(
            ILogger<TcpProxyService> logger,
            IOptions<ProxyConfiguration> config,
            IRcMessageProcessor messageProcessor,
            IConnectionManager connectionManager)
        {
            _logger = logger;
            _config = config.Value;
            _messageProcessor = messageProcessor;
            _connectionManager = connectionManager;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_isRunning)
                return;

            _cancellationTokenSource = new CancellationTokenSource();
            var combinedToken = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cancellationTokenSource.Token).Token;

            _listener = new TcpListener(IPAddress.Parse(_config.ProxyListenHost), _config.ProxyListenPort);
            _listener.Start();
            _isRunning = true;

            _logger.LogInformation($"RC_Proxy listening on {_config.ProxyListenHost}:{_config.ProxyListenPort}");
            _logger.LogInformation($"Forwarding to RC Server: {_config.RcServerHost}:{_config.RcServerPort}");

            _ = Task.Run(async () => await AcceptClientsAsync(combinedToken), combinedToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (!_isRunning)
                return;

            _logger.LogInformation("Stopping RC_Proxy server...");
            
            _isRunning = false;
            _cancellationTokenSource?.Cancel();
            _listener?.Stop();
            
            await _connectionManager.CloseAllConnectionsAsync();
            
            _logger.LogInformation("RC_Proxy server stopped");
        }

        private async Task AcceptClientsAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && _isRunning)
            {
                try
                {
                    var tcpClient = await _listener.AcceptTcpClientAsync();
                    var connectionId = Guid.NewGuid().ToString("N")[..8];
                    
                    _logger.LogInformation($"New client connection: {connectionId} from {tcpClient.Client.RemoteEndPoint}");
                    
                    // Start handling this connection in background
                    _ = Task.Run(async () => await HandleClientAsync(tcpClient, connectionId, cancellationToken), 
                                cancellationToken);
                }
                catch (ObjectDisposedException)
                {
                    // Listener was stopped
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error accepting client connection");
                }
            }
        }

        private async Task HandleClientAsync(TcpClient client, string connectionId, CancellationToken cancellationToken)
        {
            ProxyConnection connection = null;
            
            try
            {
                // Connect to RC server
                var serverClient = new TcpClient();
                await serverClient.ConnectAsync(_config.RcServerHost, _config.RcServerPort);
                
                connection = new ProxyConnection
                {
                    ConnectionId = connectionId,
                    ClientSocket = client,
                    ServerSocket = serverClient,
                    ClientStream = client.GetStream(),
                    ServerStream = serverClient.GetStream(),
                    ConnectedTime = DateTime.UtcNow
                };

                _connectionManager.AddConnection(connection);
                _logger.LogInformation($"Connection {connectionId} established to RC server");

                // Start bidirectional forwarding
                var clientToServerTask = ForwardDataAsync(
                    connection.ClientStream, connection.ServerStream, 
                    connectionId, MessageDirection.ClientToServer, cancellationToken);
                
                var serverToClientTask = ForwardDataAsync(
                    connection.ServerStream, connection.ClientStream, 
                    connectionId, MessageDirection.ServerToClient, cancellationToken);

                // Wait for either direction to complete
                await Task.WhenAny(clientToServerTask, serverToClientTask);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error handling connection {connectionId}");
            }
            finally
            {
                if (connection != null)
                {
                    _connectionManager.RemoveConnection(connectionId);
                    connection.Dispose();
                }
                _logger.LogInformation($"Connection {connectionId} closed");
            }
        }

        private async Task ForwardDataAsync(
            NetworkStream source, 
            NetworkStream destination, 
            string connectionId, 
            MessageDirection direction, 
            CancellationToken cancellationToken)
        {
            var buffer = new byte[_config.BufferSize];
            var messageBuffer = new List<byte>();

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var bytesRead = await source.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                    
                    if (bytesRead == 0)
                        break; // Connection closed

                    // Forward data immediately
                    await destination.WriteAsync(buffer, 0, bytesRead, cancellationToken);
                    await destination.FlushAsync(cancellationToken);

                    // Process messages for RabbitMQ (only server-to-client for CCG messages)
                    if (direction == MessageDirection.ServerToClient)
                    {
                        messageBuffer.AddRange(buffer.Take(bytesRead));
                        await ProcessMessageBuffer(messageBuffer, connectionId, direction);
                    }

                    if (_config.LogRawMessages)
                    {
                        _logger.LogDebug($"Forwarded {bytesRead} bytes {direction} for connection {connectionId}");
                    }
                }
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                _logger.LogError(ex, $"Error forwarding data {direction} for connection {connectionId}");
            }
        }

        private async Task ProcessMessageBuffer(List<byte> buffer, string connectionId, MessageDirection direction)
        {
            if (buffer.Count < 16) // Minimum RC message header size
                return;

            try
            {
                var messages = _messageProcessor.ExtractMessages(buffer.ToArray(), connectionId, direction);
                
                // Remove processed bytes from buffer
                if (messages.Any())
                {
                    var totalProcessedBytes = messages.Sum(m => m.RawData.Length);
                    if (totalProcessedBytes > 0 && totalProcessedBytes <= buffer.Count)
                    {
                        buffer.RemoveRange(0, totalProcessedBytes);
                    }
                }

                // Send CCG messages to RabbitMQ
                foreach (var message in messages)
                {
                    await _messageProcessor.ProcessMessageAsync(message);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Error processing message buffer for connection {connectionId}");
                // Clear buffer on error to prevent infinite loops
                buffer.Clear();
            }
        }
    }

    // RC_Proxy/Services/RcMessageProcessor.cs
    public interface IRcMessageProcessor
    {
        List<RcMessage> ExtractMessages(byte[] data, string connectionId, MessageDirection direction);
        Task ProcessMessageAsync(RcMessage message);
    }

    public class RcMessageProcessor : IRcMessageProcessor
    {
        private readonly ILogger<RcMessageProcessor> _logger;
        private readonly IRabbitMqService _rabbitMqService;
        private readonly ProxyConfiguration _config;

        public RcMessageProcessor(
            ILogger<RcMessageProcessor> logger,
            IRabbitMqService rabbitMqService,
            IOptions<ProxyConfiguration> config)
        {
            _logger = logger;
            _rabbitMqService = rabbitMqService;
            _config = config.Value;
        }

        public List<RcMessage> ExtractMessages(byte[] data, string connectionId, MessageDirection direction)
        {
            var messages = new List<RcMessage>();
            var offset = 0;

            while (offset + 16 <= data.Length) // 16 bytes = RC header size
            {
                try
                {
                    // Parse RC message header
                    var sessionName = System.Text.Encoding.ASCII.GetString(data, offset, 10).TrimEnd('\0');
                    var sequenceNumber = BitConverter.ToUInt32(data, offset + 10);
                    var blockCount = BitConverter.ToUInt16(data, offset + 14);

                    var header = new RcMessageHeader
                    {
                        SessionName = sessionName,
                        SequenceNumber = sequenceNumber,
                        BlockCount = blockCount
                    };

                    var messageLength = 16; // Header size
                    var blocks = new List<RcMessageBlock>();

                    // Parse blocks
                    var blockOffset = offset + 16;
                    for (int i = 0; i < blockCount && blockOffset < data.Length; i++)
                    {
                        if (blockOffset + 2 > data.Length)
                            break;

                        var blockLength = BitConverter.ToUInt16(data, blockOffset);
                        if (blockOffset + 2 + blockLength > data.Length)
                            break;

                        var payload = new byte[blockLength];
                        if (blockLength > 0)
                        {
                            Array.Copy(data, blockOffset + 2, payload, 0, blockLength);
                        }

                        var messageType = blockLength > 0 ? (char)payload[0] : '\0';
                        var isCcgMessage = IsCcgMessage(payload);

                        blocks.Add(new RcMessageBlock
                        {
                            Length = blockLength,
                            Payload = payload,
                            MessageType = messageType,
                            IsCcgMessage = isCcgMessage
                        });

                        messageLength += 2 + blockLength;
                        blockOffset += 2 + blockLength;
                    }

                    // Extract raw message data
                    var rawData = new byte[messageLength];
                    Array.Copy(data, offset, rawData, 0, messageLength);

                    var message = new RcMessage
                    {
                        Header = header,
                        Blocks = blocks,
                        RawData = rawData,
                        ReceivedTime = DateTime.UtcNow,
                        ConnectionId = connectionId,
                        Direction = direction
                    };

                    messages.Add(message);
                    offset += messageLength;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, $"Error parsing RC message at offset {offset}");
                    break; // Stop processing on error
                }
            }

            return messages;
        }

        public async Task ProcessMessageAsync(RcMessage message)
        {
            try
            {
                // Always publish all messages to the general queue
                await _rabbitMqService.PublishRcMessageAsync(message);

                // Process CCG messages specially
                foreach (var block in message.Blocks.Where(b => b.IsCcgMessage))
                {
                    await ProcessCcgBlock(block, message);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing message for connection {message.ConnectionId}");
            }
        }

        private async Task ProcessCcgBlock(RcMessageBlock block, RcMessage message)
        {
            if (block.MessageType != 'B' || block.Payload.Length < 3)
                return;

            try
            {
                // Extract CCG message from RC 'B' block
                var ccgLength = BitConverter.ToUInt16(block.Payload, 1);
                if (block.Payload.Length < 3 + ccgLength)
                    return;

                var ccgData = new byte[ccgLength];
                Array.Copy(block.Payload, 3, ccgData, 0, ccgLength);

                // Parse basic CCG info
                var ccgInfo = ParseCcgMessage(ccgData, message);
                
                // Publish to CCG-specific queue
                await _rabbitMqService.PublishCcgMessageAsync(ccgInfo);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing CCG block");
            }
        }

        private CcgMessageInfo ParseCcgMessage(byte[] ccgData, RcMessage parentMessage)
        {
            var ccgInfo = new CcgMessageInfo
            {
                CcgData = ccgData,
                ReceivedTime = parentMessage.ReceivedTime,
                ConnectionId = parentMessage.ConnectionId,
                SequenceNumber = parentMessage.Header.SequenceNumber
            };

            if (ccgData.Length >= 16)
            {
                try
                {
                    // Parse CCG header
                    var msgType = BitConverter.ToUInt16(ccgData, 2);
                    ccgInfo.MessageType = msgType.ToString();
                    ccgInfo.MessageName = GetCcgMessageName(msgType);

                    // Extract InstrumentId for some message types
                    ccgInfo.InstrumentId = ExtractInstrumentId(ccgData, msgType);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Error parsing CCG message details");
                }
            }

            return ccgInfo;
        }

        private bool IsCcgMessage(byte[] payload)
        {
            return payload.Length > 0 && payload[0] == (byte)'B';
        }

        private string GetCcgMessageName(ushort msgType)
        {
            return msgType switch
            {
                2 => "Login",
                3 => "LoginResponse",
                4 => "OrderAdd",
                5 => "OrderAddResponse",
                6 => "OrderCancel",
                7 => "OrderCancelResponse",
                8 => "OrderModify",
                9 => "OrderModifyResponse",
                10 => "Trade",
                11 => "Logout",
                12 => "ConnectionClose",
                13 => "Heartbeat",
                14 => "LogoutResponse",
                15 => "Reject",
                18 => "TradeCaptureReportSingle",
                19 => "TradeCaptureReportDual",
                20 => "TradeCaptureReportResponse",
                23 => "TradeBust",
                24 => "MassQuote",
                25 => "MassQuoteResponse",
                28 => "RequestForExecution",
                29 => "OrderMassCancel",
                30 => "OrderMassCancelResponse",
                31 => "BidOfferUpdate",
                32 => "MarketMakerCommand",
                33 => "MarketMakerCommandResponse",
                34 => "GapFill",
                _ => $"Unknown({msgType})"
            };
        }

        private uint? ExtractInstrumentId(byte[] ccgData, ushort msgType)
        {
            try
            {
                return msgType switch
                {
                    4 when ccgData.Length >= 21 => BitConverter.ToUInt32(ccgData, 17), // OrderAdd
                    18 when ccgData.Length >= 20 => BitConverter.ToUInt32(ccgData, 16), // TradeCaptureReportSingle
                    19 when ccgData.Length >= 20 => BitConverter.ToUInt32(ccgData, 16), // TradeCaptureReportDual
                    31 when ccgData.Length >= 20 => BitConverter.ToUInt32(ccgData, 16), // BidOfferUpdate
                    _ => null
                };
            }
            catch
            {
                return null;
            }
        }
    }
}