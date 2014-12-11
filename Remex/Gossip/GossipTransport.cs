using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Remex.Messages;
using Remex.Transport;

namespace Remex.Gossip
{
	public class GossipTransport : IDisposable
	{
		private readonly PipeOptions _options;
		readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
		private readonly TcpListenerPiper _tcpListenerPiper;

		private readonly ConcurrentDictionary<Pipe, NodeConnectionInfo> _activeView = new ConcurrentDictionary<Pipe, NodeConnectionInfo>();
		private readonly ConcurrentDictionary<Pipe, NodeConnectionInfo> _tempConnections = new ConcurrentDictionary<Pipe, NodeConnectionInfo>();
		private NodeConnectionInfo[] _passiveView = new NodeConnectionInfo[0];

		private readonly Logger _log = LogManager.GetCurrentClassLogger();

		private Task _messageHandling, _heartbeat;
		private readonly Dictionary<Type, Action<object, Pipe>> _actions;
		public GossipTransport(PipeOptions options)
		{
			_options = options;
			_tcpListenerPiper = new TcpListenerPiper(options, _cancellationTokenSource.Token);

			_messageHandling = HandleMessages();
			_heartbeat = HeartbeatTask();

			_actions = new Dictionary<Type, Action<object, Pipe>>
			{
				{typeof (JoinMessage), (o, p) => Join((JoinMessage) o, p)},
				{typeof (ForwardJoinMessage), (o, pipe) => ForwardJoin((ForwardJoinMessage) o, pipe)},
				{typeof (NeighborMessage), (o, pipe) => Neighbor((NeighborMessage) o, pipe)},
				{typeof (ActivateMessage), (o, pipe) => Activate((ActivateMessage) o, pipe)},
				{typeof (DisconnectMessage), (o, pipe) => Disconnect((DisconnectMessage) o, pipe)},
				{typeof (NeighborhoodMessage), (o, pipe) => Neighborhood((NeighborhoodMessage) o, pipe)}
			};
			_senderInfo = new NodeConnectionInfo
			{
				Host = Environment.MachineName,
				Port = _options.Port
			};
		}

		private void Neighborhood(NeighborhoodMessage msg, Pipe _)
		{
			foreach (var node in msg.Nodes)
			{
				AddNodePassiveView(node);
			}
		}

		private void Disconnect(DisconnectMessage msg, Pipe pipe)
		{
			_log.Info("Disconnected from {0}", msg.SenderInfo);
			NodeConnectionInfo _;
			_tempConnections.TryRemove(pipe, out _);
			_activeView.TryRemove(pipe, out _);
			AddNodePassiveView(msg.SenderInfo);
			pipe.DisposeWhenDoneSending();
		}

		private void Activate(ActivateMessage msg, Pipe pipe)
		{
			NodeConnectionInfo _;
			_tempConnections.TryRemove(pipe, out _);
			if (_activeView.Count == _options.MaxActiveView)
				DropRandomActiveViewNode();
			_activeView.TryAdd(pipe, msg.SenderInfo);
		}

		private async Task HeartbeatTask()
		{
			var random = new Random();
			while (_cancellationTokenSource.IsCancellationRequested == false)
			{
				await Task.Delay(_options.Heartbeat, _cancellationTokenSource.Token);
				if (_activeView.Count == 0 && _passiveView.Length == 0)
				{
					continue;
				}
				Pipe pipe;
				var next = random.Next(0, _activeView.Count + _passiveView.Length);
				if (next < _activeView.Count)
				{
					pipe = _activeView.ElementAt(next).Key;
				}
				else
				{
					var connectionInfo = _passiveView.ElementAt(next);
					pipe = new OutgoingPipe(connectionInfo, _cancellationTokenSource.Token, _options, _tcpListenerPiper);
					_tempConnections.TryAdd(pipe, null);
				}

				var priority = NeighborPriority.Low;
				if (_activeView.Count == _options.MaxActiveView)
					priority = NeighborPriority.Passive;
				else if (_activeView.Count == 0)
					priority = NeighborPriority.High;

				pipe.Write(new NeighborMessage
				{
					Priority = priority,
					SenderInfo = _senderInfo
				});
			}
		}


		private void Neighbor(NeighborMessage msg, Pipe sender)
		{
			_log.Debug("Neighbor notification from {0}", msg.SenderInfo);

			bool dropAfterSend = true;
			if (msg.Priority == NeighborPriority.High ||
				(_activeView.Count < _options.MaxActiveView && msg.Priority != NeighborPriority.Passive))
			{
				AddToActiveView(sender, msg.SenderInfo);
				dropAfterSend = false;
			}
			sender.Write(new NeighborhoodMessage
			{
				SenderInfo = _senderInfo,
				Nodes = _passiveView.Concat(_activeView.Values).ToArray()
			});

			if (dropAfterSend)
				sender.DisposeWhenDoneSending();
		}

		private void ForwardJoin(ForwardJoinMessage msg, Pipe sender)
		{
			_log.Info("Forwarded join message for {0} from {1}, ttl: {2}", msg.ForwardedNodeInfo, sender, msg.TimeToLive);
			if (KnownNode(msg.ForwardedNodeInfo))
				return;

			if (msg.TimeToLive == 0 || _activeView.Count <= 1)
			{
				var newNode = new OutgoingPipe(msg.ForwardedNodeInfo, _cancellationTokenSource.Token, _options, _tcpListenerPiper);
				AddToActiveView(newNode, msg.ForwardedNodeInfo);
				return;
			}
			if (msg.TimeToLive == _options.PassiveRandomWalkLength)
			{
				AddNodePassiveView(msg.ForwardedNodeInfo);
			}
		}

		private bool KnownNode(NodeConnectionInfo nodeConnectionInfo)
		{
			return _senderInfo.Equals(nodeConnectionInfo) ||
				   _activeView.Values.Contains(nodeConnectionInfo) ||
				   _passiveView.Contains(nodeConnectionInfo);
		}

		private void AddNodePassiveView(NodeConnectionInfo node)
		{
			_log.Info("Adding passing node {0}", node);
			if (KnownNode(node))
				return;

			if (_passiveView.Length == _options.MaxPassiveView)
			{
				var index = _messageRandom.Next(0, _passiveView.Length);
				_log.Info("Replacing passive node {0} with {1}", _passiveView[index], node);
				_passiveView[index] = node;
				return;
			}
			_passiveView = _passiveView.Concat(new[] { node }).ToArray();
		}

		private void Join(JoinMessage msg, Pipe sender)
		{
			_log.Info("Join request from {0}", msg.SenderInfo);

			AddToActiveView(sender, msg.SenderInfo);

			foreach (var kvp in _activeView)
			{
				if (kvp.Value.Equals(msg.SenderInfo))
					continue;
				kvp.Key.Write(new ForwardJoinMessage
				{
					SenderInfo = _senderInfo,
					ForwardedNodeInfo = msg.SenderInfo,
					TimeToLive = _options.ActiveRandomWalkLength
				});
			}
		}

		private void AddToActiveView(Pipe pipe, NodeConnectionInfo pipeInfo)
		{
			if (_activeView.Count == _options.MaxActiveView)
			{
				DropRandomActiveViewNode();
			}
			_passiveView = _passiveView.Where(p => p.Equals(pipeInfo) == false).ToArray();
			_activeView.TryAdd(pipe, pipeInfo);
			pipe.Write(new ActivateMessage
			{
				SenderInfo = _senderInfo
			});
		}

		private void DropRandomActiveViewNode()
		{
			var nodeToDrop = _messageRandom.Next(0, _activeView.Count);
			var pipe = _activeView.ElementAt(nodeToDrop).Key;
			NodeConnectionInfo value;
			_activeView.TryRemove(pipe, out value);

			_passiveView = _passiveView.Concat(new[] { value }).ToArray();

			pipe.Write(new DisconnectMessage
			{
				SenderInfo = new NodeConnectionInfo
				{
					Host = Environment.MachineName,
					Port = _options.Port
				}
			});
			pipe.DisposeWhenDoneSending();
		}

		private readonly Random _messageRandom = new Random();
		private NodeConnectionInfo _senderInfo;

		private async Task HandleMessages()
		{
			while (_cancellationTokenSource.IsCancellationRequested == false)
			{
				var tuple = await _tcpListenerPiper.Read(_cancellationTokenSource.Token);
				if (tuple.Item1 == null)
					continue;

				Action<object, Pipe> action;
				if (_actions.TryGetValue(tuple.Item1.GetType(), out action) == false)
				{
					action = MessageArrived;
				}

				if (action == null)
					continue;
				try
				{
					action(tuple.Item1, tuple.Item2);
				}
				catch (Exception e)
				{
					_log.Warn("Error trying to process message", e);
				}
			}
		}

		public event Action<object, Pipe> MessageArrived;

		private bool disposed;

		public async Task DisposeAsync()
		{
			if (disposed)
				return;
			disposed = true;
			_cancellationTokenSource.Cancel();
			_tcpListenerPiper.Dispose();

			foreach (var kvp in _activeView.Concat(_tempConnections))
			{
				await kvp.Key.DisposeAsync();
			}

			await _messageHandling;
			await _heartbeat;
		}

		public void Dispose()
		{
			if (disposed)
				return;
			DisposeAsync().Wait();
		}
	}
}