using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Remex.Transport;

namespace Remex
{
	public class OutgoingPipe : Pipe
	{
		private TcpClient _tcpClient;

		 private readonly NodeConnectionInfo _connectionInfo;
		private readonly TcpListenerPiper _listenerPiper;
		private readonly Task _connectionTask;

		public OutgoingPipe(NodeConnectionInfo connectionInfo, CancellationToken externalToken, PipeOptions options, TcpListenerPiper listenerPiper)
			: base(externalToken, options)
		{
			_connectionInfo = connectionInfo;
			_listenerPiper = listenerPiper;

			_connectionTask = ConnectionTask();
		}

		protected async Task ConnectionTask()
		{
			var list = new List<DateTime>();
			_tcpClient = new TcpClient();
			while (true)
			{
				try
				{
					_log.Info("Trying to connect to {0}", _connectionInfo);
					await _tcpClient.ConnectAsync(_connectionInfo.Host, _connectionInfo.Port);
					using (var stream = _tcpClient.GetStream())
					{
						var writeTask = WriteTask(stream);
						var readTask = ReadTask(stream);

						await Task.WhenAny(readTask, writeTask);
					}
				}
				catch (OperationCanceledException)
				{
					break;
				}
				catch (Exception e)
				{
					_log.Warn("Error when trying to connect to " + _connectionInfo, e);
					list.Add(DateTime.UtcNow);
					list.RemoveAll(errorTime => (DateTime.UtcNow - errorTime).TotalMinutes > 1);
					if (list.Count > 3)
					{
						_log.Warn("Too many errors in too short a time, will not attempt to retry.");
						break;
					}
				}
			}
			OnDisconnected();
			_tcpClient.Close();
		}

		protected override Task GetIncomingMessageAsync(object msg)
		{
			return _listenerPiper.GetIncomingMessageAsync(msg, this);
		}

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
			try
			{
				if (_tcpClient != null)
					_tcpClient.Close();
			}
			catch (Exception e)
			{
				errs.Add(e);
				_log.Warn("Could not close tcp client nicely", e);
			}

			if (errs.Count > 0)
				throw new AggregateException(errs);
		}
	}
}