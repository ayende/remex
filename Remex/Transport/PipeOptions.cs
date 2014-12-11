using System;
using System.Globalization;
using System.Threading;

namespace Remex
{
	public class PipeOptions
	{
		public int Port;
		public int MaxMessageLength = 16*1024;
		public int MaxActiveView = 3;
		public int MaxPassiveView = 18;
		public int ActiveRandomWalkLength = 3;
		public int PassiveRandomWalkLength = 2;
		public TimeSpan PingRecurrence = TimeSpan.FromSeconds(5);
		public TimeSpan Heartbeat = TimeSpan.FromSeconds(15);
	}
}