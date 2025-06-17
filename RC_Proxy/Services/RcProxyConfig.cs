using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace RC_Proxy.Services
{
    // Configuration classes
    public class RcProxyConfig
    {
        [Required]
        public string RcServerHost { get; set; } = "127.0.0.1";
        
        [Required]
        [Range(1, 65535)]
        public int RcServerPort { get; set; } = 19083;
        
        [Required]
        [Range(1, 65535)]
        public int ProxyListenPort { get; set; } = 19084;
        
        public string ProxyListenHost { get; set; } = "0.0.0.0";
        
        public int MaxConcurrentClients { get; set; } = 10;
        
        public int ReconnectDelayMs { get; set; } = 5000;
        
        public int HeartbeatIntervalMs { get; set; } = 30000;
    }

    public class RabbitMqConfig
    {
        [Required]
        public string HostName { get; set; } = "localhost";
        
        [Range(1, 65535)]
        public int Port { get; set; } = 5672;
        
        public string UserName { get; set; } = "guest";
        
        public string Password { get; set; } = "guest";
        
        public string VirtualHost { get; set; } = "/";
        
        [Required]
        public string CcgMessagesQueue { get; set; } = "ccg_messages";
        
        [Required]
        public string CcgMessagesExchange { get; set; } = "ccg_exchange";
        
        public string CcgMessagesRoutingKey { get; set; } = "ccg.messages";
        
        public int MaxQueueSize { get; set; } = 100000;
        
        public int MessageTtlHours { get; set; } = 24;
    }

    // RC Protocol Models (based on the specification)
    public class RcMessage
    {
        public RcHeader Header { get; set; } = new RcHeader();
        public List<RcBlock> Blocks { get; set; } = new List<RcBlock>();
        public byte[] RawData { get; set; } = Array.Empty<byte>();
        public DateTime ReceivedTime { get; set; } = DateTime.UtcNow;
    }

    public class RcHeader
    {
        public string Session { get; set; } = "";
        public uint SequenceNumber { get; set; }
        public ushort BlockCount { get; set; }
        
        public byte[] ToBytes()
        {
            var result = new byte[16];
            var sessionBytes = System.Text.Encoding.ASCII.GetBytes(Session.PadRight(10, '\0'));
            Array.Copy(sessionBytes, 0, result, 0, Math.Min(10, sessionBytes.Length));
            
            BitConverter.GetBytes(SequenceNumber).CopyTo(result, 10);
            BitConverter.GetBytes(BlockCount).CopyTo(result, 14);
            
            return result;
        }
        
        public static RcHeader FromBytes(byte[] data)
        {
            if (data.Length < 16)
                throw new ArgumentException("Invalid header data length");
            
            var header = new RcHeader();
            header.Session = System.Text.Encoding.ASCII.GetString(data, 0, 10).TrimEnd('\0');
            header.SequenceNumber = BitConverter.ToUInt32(data, 10);
            header.BlockCount = BitConverter.ToUInt16(data, 14);
            
            return header;
        }
    }

    public class RcBlock
    {
        public ushort Length { get; set; }
        public byte[] Payload { get; set; } = Array.Empty<byte>();
        
        public byte[] ToBytes()
        {
            var result = new byte[2 + Payload.Length];
            BitConverter.GetBytes(Length).CopyTo(result, 0);
            Payload.CopyTo(result, 2);
            return result;
        }
        
        public static RcBlock FromBytes(byte[] data, int offset)
        {
            if (data.Length < offset + 2)
                throw new ArgumentException("Invalid block data length");
            
            var block = new RcBlock();
            block.Length = BitConverter.ToUInt16(data, offset);
            
            if (data.Length < offset + 2 + block.Length)
                throw new ArgumentException("Invalid block payload length");
            
            block.Payload = new byte[block.Length];
            Array.Copy(data, offset + 2, block.Payload, 0, block.Length);
            
            return block;
        }
    }

    // CCG Message stored in RabbitMQ
    public class StoredCcgMessage
    {
        public uint SequenceNumber { get; set; }
        public byte[] CcgData { get; set; } = Array.Empty<byte>();
        public DateTime StoredTime { get; set; } = DateTime.UtcNow;
        public string SessionId { get; set; } = "";
        public ushort MessageType { get; set; }
        public uint InstrumentId { get; set; }
        public string MessageName { get; set; } = "";
        
        public byte[] ToBytes()
        {
            return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(this);
        }
        
        public static StoredCcgMessage FromBytes(byte[] data)
        {
            return System.Text.Json.JsonSerializer.Deserialize<StoredCcgMessage>(data) 
                ?? throw new InvalidOperationException("Failed to deserialize StoredCcgMessage");
        }
    }

    // Client connection info
    public class ClientConnectionInfo
    {
        public string ClientId { get; set; } = Guid.NewGuid().ToString();
        public DateTime ConnectedTime { get; set; } = DateTime.UtcNow;
        public string RemoteEndpoint { get; set; } = "";
        public bool IsAuthenticated { get; set; } = false;
        public string SessionId { get; set; } = "";
        public uint LastSentSequenceNumber { get; set; } = 0;
        public uint LastReceivedSequenceNumber { get; set; } = 0;
    }

    // Rewind request
    public class RewindRequest
    {
        public string ClientId { get; set; } = "";
        public uint LastSeenSequenceNumber { get; set; }
        public DateTime RequestTime { get; set; } = DateTime.UtcNow;
    }

    // Message types for routing
    public enum MessageDirection
    {
        ClientToRc,
        RcToClient,
        Internal
    }

    public class RoutedMessage
    {
        public MessageDirection Direction { get; set; }
        public string ClientId { get; set; } = "";
        public RcMessage Message { get; set; } = new RcMessage();
        public bool ShouldForwardToRc { get; set; } = true;
        public bool ShouldForwardToClient { get; set; } = true;
        public DateTime ProcessedTime { get; set; } = DateTime.UtcNow;
    }
}