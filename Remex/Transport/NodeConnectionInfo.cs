// -----------------------------------------------------------------------
//  <copyright file="NodeConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Remex.Transport
{
	public class NodeConnectionInfo
	{
		protected bool Equals(NodeConnectionInfo other)
		{
			return string.Equals(Host, other.Host) && Port == other.Port;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((NodeConnectionInfo) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return ((Host != null ? Host.GetHashCode() : 0)*397) ^ Port;
			}
		}

		public string Host { get; set; }
		public int Port { get; set; }

		public override string ToString()
		{
			return string.Format("Host: {0}, Port: {1}", Host, Port);
		}
	}
}