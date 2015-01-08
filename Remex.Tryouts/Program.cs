using System;
using System.IO;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;
using Remex.Gossip;
using Remex.Transport;

namespace Remex.Tryouts
{
	class Program
	{
		static void Main(string[]args)
		{
			if (args.Length == 0)
			{
				var g1 = new GossipTransport(new PipeOptions
				{
					Port = 0,
				});
				Console.WriteLine(g1.Options.Port);
				Console.ReadLine();
			}

			var g2 = new GossipTransport(new PipeOptions
			{
				Port = 0,
			});
			g2.Join(new NodeConnectionInfo
			{
				Port = int.Parse(args[0]),
				Host = "localhost"
			});
			Console.WriteLine(g2.Options.Port);
			Console.ReadLine();

		}
	}


	public class MyMsg
	{
		public int i;
	}
}
