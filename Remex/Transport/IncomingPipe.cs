using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Remex
{
	public class IncomingPipe : Pipe
	{
		private readonly TcpListenerPiper _parent;
		private readonly Task _connectionTask;
		private readonly string _description;

		public IncomingPipe(CancellationToken externalToken, PipeOptions options, TcpClient client, TcpListenerPiper parent) : base(externalToken, options)
		{
			_parent = parent;
			_description = client.Client.RemoteEndPoint.ToString();
			_connectionTask = ConnectionTask(client);
		}

		private async Task ConnectionTask(TcpClient client)
		{
			try
			{
				using (var stream = client.GetStream())
				{
					var writeTask = WriteTask(stream);
					var readTask = ReadTask(stream);

					await Task.WhenAny(readTask, writeTask);
				}
			}
			catch (OperationCanceledException)
			{
			}
		}

		protected override Task GetIncomingMessageAsync(object msg)
		{
			return _parent.GetIncomingMessageAsync(msg, this);
		}

		public override string Description { get { return _description; } }

		public async override Task DisposeAsync()
		{
			var errs = new List<Exception>();

			try
			{
				await base.DisposeAsync();
			}
			catch (Exception e)
			{
				errs.Add(e);
				_log.Warn("Could not dispose of pipe propertly", e);
			}
			try
			{
				await _connectionTask;
			}
			catch (Exception e)
			{
				errs.Add(e);
				_log.Warn("Could not shut down conenction properly", e);
			}

			if (errs.Count > 0)
				throw new AggregateException(errs);
		}
	}
}