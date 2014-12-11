using System.Threading;
using Newtonsoft.Json;
using Remex.Transport;
using Xunit;

namespace Remex.Tests
{
    public class PipeTests
    {
	    [Fact]
	    public void CanCreateAndDisposePiper()
	    {
		    using (new TcpListenerPiper(new PipeOptions(), CancellationToken.None))
		    {
			    
		    }
	    }

		[Fact]
		public void CanConnectToPiper()
		{
			var pipeOptions = new PipeOptions();
			using (var pipeListener = new TcpListenerPiper(pipeOptions, CancellationToken.None))
			{
				var outgoingPipe = new OutgoingPipe(new NodeConnectionInfo
				{
					Host = "localhost",
					Port = pipeOptions.Port
				}, CancellationToken.None, new PipeOptions(), pipeListener);

				outgoingPipe.Write("hello there");

				var read = pipeListener.Read(CancellationToken.None).Result;
				Assert.Equal("hello there", read.Item1);
			}
		}

		[Fact]
		public void CanConnectToPiper_AndSendSeveralMessages()
		{
			var pipeOptions = new PipeOptions{Port = 22222};
			using (var pipeListener = new TcpListenerPiper(pipeOptions, CancellationToken.None))
			{
				var outgoingPipe = new OutgoingPipe(new NodeConnectionInfo
				{
					Host = "localhost",
					Port = pipeOptions.Port
				}, CancellationToken.None, new PipeOptions(),pipeListener);

				for (long i = 0; i < 10; i++)
				{
					outgoingPipe.Write(i);
				}

				for (long i = 0; i < 10; i++)
				{
					var read = pipeListener.Read(CancellationToken.None).Result;
					Assert.Equal(i, read.Item1);
				}
			}
		}

		[Fact]
		public void CanConnectToPiper_Messages()
		{
			var pipeOptions = new PipeOptions { Port = 22222 };
			using (var pipeListener = new TcpListenerPiper(pipeOptions, CancellationToken.None))
			{
				var outgoingPipe = new OutgoingPipe(new NodeConnectionInfo
				{
					Host = "localhost",
					Port = pipeOptions.Port
				}, CancellationToken.None, new PipeOptions(),pipeListener);

				for (int i = 0; i < 10; i++)
				{
					outgoingPipe.Write(new MyMsg{i =i});
				}

				for (int i = 0; i < 10; i++)
				{
					var read = pipeListener.Read(CancellationToken.None).Result;
					Assert.Equal(i, ((MyMsg) read.Item1).i);
				}
			}
		}

		[JsonObject(ItemTypeNameHandling = TypeNameHandling.Objects)]
	    public class MyMsg
	    {
		    public int i;
	    }
    }

}
