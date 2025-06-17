// RC_Proxy/Models/RcMessage.cs
namespace RC_Proxy.Models
{
    public class RcMessage
    {
        public RcMessageHeader Header { get; set; }
        public List<RcMessageBlock> Blocks { get; set; } = new();
        public byte[] RawData { get; set; }
        public DateTime ReceivedTime { get; set; }
        public string ConnectionId { get; set; }
        public MessageDirection Direction { get; set; }
    }

    public class RcMessageHeader
    {
        public string SessionName { get; set; }
        public uint SequenceNumber { get; set; }
        public ushort BlockCount { get; set; }
    }

    public class RcMessageBlock
    {
        public ushort Length { get; set; }
        public byte[] Payload { get; set; }
        public char MessageType { get; set; }
        public bool IsCcgMessage { get; set; }
    }

    public enum MessageDirection
    {
        ClientToServer,  // RC_GUI -> RC
        ServerToClient   // RC -> RC_GUI
    }

    public class CcgMessageInfo
    {
        public uint SequenceNumber { get; set; }
        public byte[] CcgData { get; set; }
        public DateTime ReceivedTime { get; set; }
        public string ConnectionId { get; set; }
        public string MessageType { get; set; }
        public uint? InstrumentId { get; set; }
        public string MessageName { get; set; }
    }
}