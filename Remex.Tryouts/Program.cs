using System;
using System.IO;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;

namespace Remex.Tryouts
{
	class Program
	{
		static void Main()
		{
			var b = new BufferBlock<string>();
			b.SendAsync("test").Wait();
			Console.WriteLine(b.Completion.IsCompleted);
			var receiveAsync = b.ReceiveAsync(TimeSpan.FromSeconds(2)).Result;
			Console.WriteLine(receiveAsync);

			receiveAsync = b.ReceiveAsync(TimeSpan.FromSeconds(2)).Result;
			Console.WriteLine(receiveAsync);
		}
	}


	public class MyMsg
	{
		public int i;
	}
}
