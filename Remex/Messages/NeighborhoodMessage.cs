using Remex.Transport;

namespace Remex.Messages
{
	public class NeighborhoodMessage : BaseMessage
	{
		public NodeConnectionInfo[] Nodes { get; set; }
	}
}