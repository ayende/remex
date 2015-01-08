using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Newtonsoft.Json;
using NLog;

namespace Remex
{
	public abstract class Pipe : IDisposable
	{
		private readonly CancellationTokenSource _cancellationToken;

		private readonly BufferBlock<object> _outgoing = new BufferBlock<object>();

		private readonly PipeOptions _options;

		public event EventHandler Disconnected;

		protected virtual void OnDisconnected()
		{
			var handler = Disconnected;
			if (handler != null) handler(this, EventArgs.Empty);
		}

		protected readonly Logger _log = LogManager.GetCurrentClassLogger();

		protected Pipe(CancellationToken externalToken, PipeOptions options)
		{
			_options = options;
			_cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(new[] { externalToken });
		}

		protected async Task ReadTask(NetworkStream stream)
		{
			var readBuffer = new byte[1024];
			var serializer = new JsonSerializer
			{
				TypeNameHandling = TypeNameHandling.Auto
			};
			var readingLength = true;
			var usedBuffer = 0;
			var currentLength = -1;

			var readMsgTask = stream.ReadAsync(readBuffer, 0, readBuffer.Length, _cancellationToken.Token);
			while (_cancellationToken.IsCancellationRequested == false)
			{
				var read = await readMsgTask;
				if (read == 0)
					throw new EndOfStreamException();
				usedBuffer += read;
				if (readingLength)
				{
					if (usedBuffer > 3)
					{
						for (int i = 0; i < usedBuffer - 1; i++)
						{
							if (readBuffer[i] == (byte)'\r' && readBuffer[i + 1] == (byte)'\n')
							{
								var lenAsStr = Encoding.UTF8.GetString(readBuffer, 0, i);

								if (int.TryParse(lenAsStr, out currentLength) == false)
								{
									throw new InvalidOperationException("Got invalid message length " + lenAsStr);
								}
								if (currentLength > _options.MaxMessageLength)
									throw new InvalidOperationException("Maximum message size exceeded: " + currentLength);
								if (currentLength > readBuffer.Length)
									readBuffer = new byte[currentLength];

								readingLength = false;
								if (usedBuffer == i + 2)
								{
									readMsgTask = stream.ReadAsync(readBuffer, 0, readBuffer.Length, _cancellationToken.Token);
								}
								else
								{
									Buffer.BlockCopy(readBuffer, i + 2, readBuffer, 0, usedBuffer);
									readMsgTask = Task.FromResult(usedBuffer - i - 2);
								}
								usedBuffer = 0;
								break;

							}
						}
						if (readingLength == false)
							continue;
					}
					if (usedBuffer > 16)
						throw new InvalidOperationException("Could not find message length after reading 16 bytes");
					// read some more
					readMsgTask = stream.ReadAsync(readBuffer, usedBuffer, readBuffer.Length - usedBuffer, _cancellationToken.Token);
					continue;
				}
				if (usedBuffer < currentLength)
				{
					readMsgTask = stream.ReadAsync(readBuffer, usedBuffer, readBuffer.Length - usedBuffer, _cancellationToken.Token);
					continue;
				}
				var msg = serializer.Deserialize(new StreamReader(new MemoryStream(readBuffer, 0, currentLength)), typeof(object));
				if (msg != null)
					await GetIncomingMessageAsync(msg);

				readingLength = true;

				var remainingLength = usedBuffer - currentLength;
				if (remainingLength > 0)
				{
					Buffer.BlockCopy(readBuffer, currentLength, readBuffer, 0, remainingLength);
				}

				usedBuffer = 0;
				currentLength = -1;

				readMsgTask = remainingLength > 0 ?
					Task.FromResult(remainingLength) :
					stream.ReadAsync(readBuffer, 0, readBuffer.Length, _cancellationToken.Token);
			}
		}

		protected abstract Task GetIncomingMessageAsync(object msg);

		protected async Task WriteTask(NetworkStream stream)
		{
			var serializer = new JsonSerializer
			{
				Formatting = Formatting.None,
				TypeNameHandling = TypeNameHandling.Auto
			};
			var writeBuffer = new MemoryStream();
			var lenBuffer = new byte[16];
			while (_cancellationToken.IsCancellationRequested == false)
			{
				writeBuffer.SetLength(0);
				var streamWriter = new StreamWriter(writeBuffer);
				bool shouldDispose = false;
				try
				{
					var msg = await _outgoing.ReceiveAsync(_options.PingRecurrence, _cancellationToken.Token);
					_log.Debug("Writing {0} to {1}", msg, Description);
					serializer.Serialize(streamWriter, msg, typeof(object));
				}
				catch (TimeoutException)
				{
					if (_cancellationToken.IsCancellationRequested)
					{
						shouldDispose = true;
					}
					// we proceed with an empty message
				}
				catch (Exception e)
				{
					_log.Warn("Got an error reading messages", e);
					// this probably indicate that we are broken in some manner
					shouldDispose = true;
				}
				if (shouldDispose)
				{
					await DisposeAsync();
					return;
				}
				streamWriter.WriteLine();
				streamWriter.Flush();

				var lenAsStr = writeBuffer.Length.ToString(CultureInfo.InvariantCulture);
				var size = Encoding.UTF8.GetBytes(lenAsStr, 0, lenAsStr.Length, lenBuffer, 0);
				if (size + 2 >= lenBuffer.Length)
					throw new IOException("Message size is WAY too large");
				lenBuffer[size] = (byte)'\r';
				lenBuffer[size + 1] = (byte)'\n';

				await stream.WriteAsync(lenBuffer, 0, size + 2);
				await stream.WriteAsync(writeBuffer.GetBuffer(), 0, (int)writeBuffer.Length);

				if (_outgoing.Count == 0)
					await stream.FlushAsync(_cancellationToken.Token);
			}
		}

		public abstract string Description { get; }

		public void Write(object msg)
		{
			if (msg == null) throw new ArgumentNullException("msg");
			if (_outgoing.Post(msg) == false)
				throw new InvalidOperationException("Cannot post message to pipe");
		}

		private bool disposed;

		public virtual Task DisposeAsync()
		{
			if (disposed)
				return Task.FromResult(1);
			disposed = true;
			_cancellationToken.Cancel();
			_outgoing.Complete();
			return Task.FromResult(1);
		}

		public void Dispose()
		{
			if (disposed)
				return;
			DisposeAsync().Wait();
		}

		public void DisposeWhenDoneSending()
		{
			_outgoing.Complete();
		}
	}
}
