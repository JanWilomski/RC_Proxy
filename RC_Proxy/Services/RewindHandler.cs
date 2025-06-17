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
                var sentCount = 0;
                var errorCount = 0;
                
                foreach (var storedMessage in storedMessages.OrderBy(m => m.SequenceNumber))
                {
                    try
                    {
                        // Create RC message from stored CCG message
                        var rcMessage = CreateRcMessageFromStoredMessage(storedMessage, sequenceNumber++);
                        
                        // Validate the created message
                        if (rcMessage.RawData == null || rcMessage.RawData.Length == 0)
                        {
                            _logger.LogError("Created RC message has no raw data for stored message {SeqNum}", storedMessage.SequenceNumber);
                            errorCount++;
                            continue;
                        }
                        
                        // Send to client
                        await clientStream.WriteAsync(rcMessage.RawData);
                        await clientStream.FlushAsync();
                        sentCount++;
                        
                        _logger.LogTrace("Sent rewind message {SeqNum} to client {ClientId} ({Size} bytes)", 
                            rcMessage.Header.SequenceNumber, request.ClientId, rcMessage.RawData.Length);
                        
                        // Small delay to prevent overwhelming the client
                        if (sentCount % 50 == 0)
                        {
                            await Task.Delay(5);
                        }
                    }
                    catch (Exception ex)
                    {
                        errorCount++;
                        _logger.LogError(ex, "Failed to send rewind message with original sequence {SeqNum} to client {ClientId}", 
                            storedMessage.SequenceNumber, request.ClientId);
                        
                        // If too many errors, stop sending
                        if (errorCount > 10)
                        {
                            _logger.LogError("Too many errors ({ErrorCount}) sending rewind messages, stopping", errorCount);
                            break;
                        }
                    }
                }

                // Send rewind complete message
                var lastSessionId = storedMessages.LastOrDefault()?.SessionId ?? "";
                await SendRewindCompleteAsync(clientStream, lastSessionId);

                if (errorCount > 0)
                {
                    _logger.LogWarning("Completed rewind for client {ClientId}: sent {SentCount}/{TotalCount} messages ({ErrorCount} errors)", 
                        request.ClientId, sentCount, storedMessages.Count, errorCount);
                }
                else
                {
                    _logger.LogInformation("Completed rewind for client {ClientId}, sent {Count} messages successfully", 
                        request.ClientId, sentCount);
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to handle rewind request for client {ClientId}", request.ClientId);
                return false;
            }
        }

        // 4. DODATKOWA METODA DIAGNOSTYCZNA
        public async Task<Dictionary<string, object>> GetRewindDiagnosticsAsync(uint fromSequenceNumber = 0, int sampleSize = 5)
        {
            var diagnostics = new Dictionary<string, object>();
            
            try
            {
                // Get sample messages from RabbitMQ
                var storedMessages = await _rabbitMqService.GetCcgMessagesAsync(fromSequenceNumber, sampleSize);
                
                diagnostics["SampleSize"] = sampleSize;
                diagnostics["FoundMessages"] = storedMessages.Count;
                diagnostics["FromSequenceNumber"] = fromSequenceNumber;
                
                var sampleAnalysis = new List<object>();
                
                foreach (var storedMessage in storedMessages.Take(sampleSize))
                {
                    try
                    {
                        // Test reconstruction
                        var rcMessage = CreateRcMessageFromStoredMessage(storedMessage, storedMessage.SequenceNumber);
                        
                        sampleAnalysis.Add(new
                        {
                            OriginalSeqNum = storedMessage.SequenceNumber,
                            SessionId = storedMessage.SessionId,
                            MessageType = storedMessage.MessageType,
                            MessageName = storedMessage.MessageName,
                            CcgDataSize = storedMessage.CcgData?.Length ?? 0,
                            ReconstructedSize = rcMessage.RawData?.Length ?? 0,
                            ReconstructionSuccess = rcMessage.RawData != null && rcMessage.RawData.Length > 0
                        });
                    }
                    catch (Exception ex)
                    {
                        sampleAnalysis.Add(new
                        {
                            OriginalSeqNum = storedMessage.SequenceNumber,
                            Error = ex.Message,
                            ReconstructionSuccess = false
                        });
                    }
                }
                
                diagnostics["SampleAnalysis"] = sampleAnalysis;
                diagnostics["Timestamp"] = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                diagnostics["Error"] = ex.Message;
                diagnostics["StackTrace"] = ex.StackTrace;
            }
            
            return diagnostics;
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
            try
            {
                // Create RC message header
                var rcMessage = new RcMessage
                {
                    Header = new RcHeader
                    {
                        Session = storedMessage.SessionId,
                        SequenceNumber = sequenceNumber, // Use provided sequence number for rewind
                        BlockCount = 1
                    },
                    ReceivedTime = storedMessage.StoredTime
                };
                
                // Get the original CCG data
                var ccgData = storedMessage.CcgData;
                
                if (ccgData == null || ccgData.Length == 0)
                {
                    throw new InvalidOperationException($"Empty CCG data for stored message {storedMessage.SequenceNumber}");
                }
                
                // Create CCG message block with proper RC protocol format
                // RC Protocol format for CCG messages: 'B' + uint16 length + CCG data
                var blockPayload = new byte[3 + ccgData.Length];
                blockPayload[0] = (byte)'B';  // CCG message identifier
                BitConverter.GetBytes((ushort)ccgData.Length).CopyTo(blockPayload, 1);  // Length of CCG data
                ccgData.CopyTo(blockPayload, 3);  // Actual CCG data
                
                var ccgBlock = new RcBlock
                {
                    Length = (ushort)blockPayload.Length,
                    Payload = blockPayload
                };
                
                rcMessage.Blocks.Add(ccgBlock);
                
                // Serialize the complete message to RawData
                rcMessage.RawData = SerializeRcMessage(rcMessage);
                
                _logger.LogTrace("Created RC message from stored CCG message: SeqNum={OriginalSeq}->{NewSeq}, Size={Size} bytes", 
                    storedMessage.SequenceNumber, sequenceNumber, rcMessage.RawData.Length);
                
                return rcMessage;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create RC message from stored CCG message {SeqNum}", storedMessage.SequenceNumber);
                throw new InvalidOperationException($"Failed to create RC message: {ex.Message}", ex);
            }
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
            // Format: 'B' + uint16 length + CCG binary data
            if (ccgBlock.Payload.Length >= 3)
            {
                ushort ccgLength = BitConverter.ToUInt16(ccgBlock.Payload, 1);
                
                if (ccgBlock.Payload.Length >= 3 + ccgLength)
                {
                    // Extract the actual CCG binary message (skip the 'B' and length prefix)
                    var ccgData = new byte[ccgLength];
                    Array.Copy(ccgBlock.Payload, 3, ccgData, 0, ccgLength);
                    
                    // Store the CCG data (this will convert to Base64 automatically)
                    storedMessage.CcgData = ccgData;
                    
                    // Parse CCG message type and instrument ID if available
                    if (ccgData.Length >= 4)
                    {
                        // CCG message format: uint16 length + uint16 msgType + ...
                        storedMessage.MessageType = BitConverter.ToUInt16(ccgData, 2);
                        
                        // Try to extract instrument ID (depends on message type)
                        if (ccgData.Length >= 20) // Most CCG messages have instrumentId at offset 16
                        {
                            try
                            {
                                storedMessage.InstrumentId = BitConverter.ToUInt32(ccgData, 16);
                            }
                            catch
                            {
                                storedMessage.InstrumentId = 0;
                            }
                        }
                        
                        // Set message name based on type
                        storedMessage.MessageName = GetCcgMessageName(storedMessage.MessageType);
                    }
                }
                else
                {
                    throw new ArgumentException($"CCG block payload too short: expected {3 + ccgLength} bytes, got {ccgBlock.Payload.Length}");
                }
            }
            else
            {
                throw new ArgumentException("CCG block payload too short to contain length header");
            }

            return storedMessage;
        }
        private static string GetCcgMessageName(ushort messageType)
        {
            return messageType switch
            {
                // Common CCG message types based on GPW specification
                1 => "Login",
                2 => "LoginResponse", 
                3 => "Logout",
                4 => "Heartbeat",
                5 => "OrderAdd",
                6 => "OrderAddResponse",
                7 => "OrderModify",
                8 => "OrderModifyResponse",
                9 => "OrderCancel",
                10 => "OrderCancelResponse",
                11 => "Trade",
                12 => "OrderMassCancel",
                13 => "OrderMassCancelResponse",
                14 => "TradeCaptureReportSingle",
                15 => "TradeCaptureReportDual",
                16 => "TradeCaptureReportResponse",
                17 => "MarketDataSnapshot",
                18 => "MarketDataIncrementalRefresh",
                19 => "BidOfferUpdate",
                20 => "ConnectionClose",
                21 => "GapFill",
                _ => $"Unknown({messageType})"
            };
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