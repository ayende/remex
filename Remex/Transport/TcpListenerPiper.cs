using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Remex
{
	public class TcpListenerPiper : IDisposable
	{
		private readonly PipeOptions _options;
		private readonly TcpListener _tcpListener;

		private readonly Task _acceptConnectionsTask;
		private readonly CancellationTokenSource _cancellationTokenSource;

		private readonly BufferBlock<Tuple<object, Pipe>> _incoming = new BufferBlock<Tuple<object, Pipe>>();

		private readonly ConcurrentDictionary<IncomingPipe, object> _pipes = new ConcurrentDictionary<IncomingPipe, object>();


		public TcpListenerPiper(PipeOptions options, CancellationToken token)
		{
			_options = options;

			_cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(new []{token});

			_tcpListener = new TcpListener(IPAddress.Any, options.Port);
			_tcpListener.Start();
			options.Port = ((IPEndPoint) _tcpListener.LocalEndpoint).Port;
			_acceptConnectionsTask = AcceptConnections();
		}

		public Task<Tuple<object, Pipe>> Read(CancellationToken token)
		{
			return _incoming.ReceiveAsync(token);
		}

		private async Task AcceptConnections()
		{
			TcpClient tcpClient;
			try
			{
				tcpClient = await _tcpListener.AcceptTcpClientAsync();
			}
			catch (ObjectDisposedException)
			{
				return;
			}

			var pipe = new IncomingPipe(_cancellationTokenSource.Token, _options, tcpClient, this);
			_pipes.TryAdd(pipe, null);
		}

		private bool disposed;
		public async Task DisposeAsync()
		{
			if (disposed)
				return;
			disposed = true;
			_incoming.Complete();
			_cancellationTokenSource.Cancel();
			_tcpListener.Stop();
			foreach (var pipe in _pipes)
			{
				await pipe.Key.DisposeAsync();
			}
			await _acceptConnectionsTask;
		}

		public void Dispose()
		{
			if (disposed)
				return;
			DisposeAsync().Wait();

		}

		internal Task GetIncomingMessageAsync(object msg, Pipe incomingPipe)
		{
			return _incoming.SendAsync(Tuple.Create(msg, incomingPipe));
		}
	}
}