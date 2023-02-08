using System;
using Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class UserAgent
    {
        private readonly string _version;
        private readonly string _platform;
        private readonly string _hostName;

        public static readonly UserAgent Instance = new();

        private UserAgent()
        {
            _version = MetadataConstants.Instance.ClientVersion;
            _platform = Environment.OSVersion.ToString();
            _hostName = System.Net.Dns.GetHostName();
        }

        public UA ToProtobuf()
        {
            return new UA
            {
                Version = _version,
                Hostname = _hostName,
                Platform = _platform
            };
        }
    }
}