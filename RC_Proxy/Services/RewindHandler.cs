using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace RC_Proxy.Services
{
    public interface IRewindHandler
    {
        Task<bool> HandleRewindRequestAsync(RewindRequest request, NetworkStream clientStream);
        Task<List<RcMessage>> CreateRewindMessagesAsync(uint fromSequenceNumber, string sessionId);
    }

    public class RewindHandler : IRewindHandler
    {
        private readonly IRabbitMqService _rabbitMqService;
        private readonly ILogger<RewindHandler> _logger;
        
        public RewindHandler(IRabbitMqService rabbitMqService, ILogger<RewindHandler> logger)
        {
            _rabbitMqService = rabbitMqService;
            _logger = logger;
        }

        public async Task<bool> HandleRewindRequestAsync(RewindRequest request, NetworkStream clientStream)
        {
            try
            {
                _logger.LogInformation("Handling rewind request from client {ClientId}, from sequence {FromSeq}", 
                    request.ClientId, request.LastSeenSequenceNumber);

                // Get messages from RabbitMQ
                var storedMessages = await _rabbitMqService.GetCcgMessagesAsync(
                    request.LastSeenSequenceNumber + 1, 10000); // Get up to 10k messages

                if (storedMessages.Count == 0)
                {
                    _logger.LogInformation("No messages to rewind for client {ClientId}", request.ClientId);
                    
                    // Send rewind complete message
                    await SendRewindCompleteAsync(clientStream, "");
                    return true;
                }

                _logger.LogInformation("Sending {Count} messages to client {ClientId} for rewind", 
                    storedMessages.Count, request.ClientId);

                // Send stored messages
                uint sequenceNumber = request.LastSeenSequenceNumber + 1;
                foreach (var storedMessage in storedMessages.OrderBy(m => m.SequenceNumber))
                {
                    try
                    {
                        // Create RC message from stored CCG message
                        var rcMessage = CreateRcMessageFromStoredMessage(storedMessage, sequenceNumber++);
                        
                        // Send to client
                        var messageBytes = SerializeRcMessage(rcMessage);
                        await clientStream.WriteAsync(messageBytes);
                        await clientStream.FlushAsync();
                        
                        // Small delay to prevent overwhelming the client
                        if (sequenceNumber % 100 == 0)
                        {
                            await Task.Delay(10);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to send rewind message with sequence {SeqNum}", 
                            storedMessage.SequenceNumber);
                    }
                }

                // Send rewind complete message
                await SendRewindCompleteAsync(clientStream, storedMessages.LastOrDefault()?.SessionId ?? request.ClientId);

                _logger.LogInformation("Completed rewind for client {ClientId}, sent {Count} messages", 
                    request.ClientId, storedMessages.Count);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to handle rewind request for client {ClientId}", request.ClientId);
                return false;
            }
        }

        public async Task<List<RcMessage>> CreateRewindMessagesAsync(uint fromSequenceNumber, string sessionId)
        {
            var messages = new List<RcMessage>();
            
            try
            {
                var storedMessages = await _rabbitMqService.GetCcgMessagesAsync(fromSequenceNumber);
                
                uint sequenceNumber = fromSequenceNumber;
                foreach (var storedMessage in storedMessages.OrderBy(m => m.SequenceNumber))
                {
                    var rcMessage = CreateRcMessageFromStoredMessage(storedMessage, sequenceNumber++);
                    messages.Add(rcMessage);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create rewind messages from sequence {FromSeq}", fromSequenceNumber);
                throw;
            }
            
            return messages;
        }

        private RcMessage CreateRcMessageFromStoredMessage(StoredCcgMessage storedMessage, uint sequenceNumber)
        {
            var rcMessage = new RcMessage
            {
                Header = new RcHeader
                {
                    Session = storedMessage.SessionId,
                    SequenceNumber = sequenceNumber,
                    BlockCount = 1
                },
                ReceivedTime = storedMessage.StoredTime
            };
            
            // Create CCG message block
            var ccgBlock = new RcBlock
            {
                Length = (ushort)storedMessage.CcgData.Length,
                Payload = storedMessage.CcgData
            };
            
            rcMessage.Blocks.Add(ccgBlock);
            
            // Serialize the complete message
            rcMessage.RawData = SerializeRcMessage(rcMessage);
            
            return rcMessage;
        }

        private byte[] SerializeRcMessage(RcMessage message)
        {
            var headerBytes = message.Header.ToBytes();
            var blockBytes = new List<byte>();
            
            foreach (var block in message.Blocks)
            {
                blockBytes.AddRange(block.ToBytes());
            }
            
            var result = new byte[headerBytes.Length + blockBytes.Count];
            headerBytes.CopyTo(result, 0);
            blockBytes.ToArray().CopyTo(result, headerBytes.Length);
            
            return result;
        }

        private async Task SendRewindCompleteAsync(NetworkStream stream, string sessionId)
        {
            try
            {
                // Create rewind complete message ('r' message type)
                var rewindCompleteMessage = new RcMessage
                {
                    Header = new RcHeader
                    {
                        Session = sessionId,
                        SequenceNumber = 0, // Session message
                        BlockCount = 1
                    }
                };

                // Rewind complete block
                var rewindCompleteBlock = new RcBlock
                {
                    Length = 1,
                    Payload = new byte[] { (byte)'r' }
                };

                rewindCompleteMessage.Blocks.Add(rewindCompleteBlock);

                var messageBytes = SerializeRcMessage(rewindCompleteMessage);
                await stream.WriteAsync(messageBytes);
                await stream.FlushAsync();

                _logger.LogDebug("Sent rewind complete message");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send rewind complete message");
                throw;
            }
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

        public static RewindRequest ParseRewindRequest(RcMessage message)
        {
            var request = new RewindRequest();
            
            // Find the rewind block ('R' message type)
            var rewindBlock = message.Blocks.FirstOrDefault(b => 
                b.Payload.Length > 0 && b.Payload[0] == (byte)'R');
            
            if (rewindBlock != null && rewindBlock.Payload.Length >= 5)
            {
                // Extract last seen sequence number (uint32 at offset 1)
                request.LastSeenSequenceNumber = BitConverter.ToUInt32(rewindBlock.Payload, 1);
            }
            
            return request;
        }

        public static bool IsRewindRequest(RcMessage message)
        {
            return message.Blocks.Any(b => 
                b.Payload.Length > 0 && b.Payload[0] == (byte)'R');
        }

        public static bool IsCcgMessage(RcMessage message)
        {
            return message.Blocks.Any(b => 
                b.Payload.Length > 0 && b.Payload[0] == (byte)'B');
        }

        public static StoredCcgMessage ExtractCcgMessage(RcMessage rcMessage)
        {
            var ccgBlock = rcMessage.Blocks.FirstOrDefault(b => 
                b.Payload.Length > 0 && b.Payload[0] == (byte)'B');
            
            if (ccgBlock == null)
                throw new ArgumentException("No CCG message block found");

            var storedMessage = new StoredCcgMessage
            {
                SequenceNumber = rcMessage.Header.SequenceNumber,
                SessionId = rcMessage.Header.Session,
                StoredTime = rcMessage.ReceivedTime
            };

            // Extract CCG data from the block
            if (ccgBlock.Payload.Length >= 3)
            {
                ushort ccgLength = BitConverter.ToUInt16(ccgBlock.Payload, 1);
                if (ccgBlock.Payload.Length >= 3 + ccgLength)
                {
                    storedMessage.CcgData = new byte[ccgLength];
                    Array.Copy(ccgBlock.Payload, 3, storedMessage.CcgData, 0, ccgLength);
                    
                    // Parse message type and other info if possible
                    if (ccgLength >= 4)
                    {
                        storedMessage.MessageType = BitConverter.ToUInt16(storedMessage.CcgData, 2);
                        storedMessage.MessageName = GetMessageTypeName(storedMessage.MessageType);
                        
                        // Extract instrument ID if present
                        if (ccgLength >= 21)
                        {
                            storedMessage.InstrumentId = BitConverter.ToUInt32(storedMessage.CcgData, 17);
                        }
                    }
                }
            }

            return storedMessage;
        }

        private static string GetMessageTypeName(ushort messageType)
        {
            return messageType switch
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
                _ => $"Unknown({messageType})"
            };
        }
    }
}