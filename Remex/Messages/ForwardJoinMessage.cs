using Remex.Transport;

namespace Remex.Messages
{
	public class ForwardJoinMessage : BaseMessage
	{
		public NodeConnectionInfo ForwardedNodeInfo { get; set; }
		public int TimeToLive { get; set; }
	}
}