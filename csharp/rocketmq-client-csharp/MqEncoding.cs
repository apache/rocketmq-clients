using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public enum MqEncoding
    {
        Identity,
        Gzip
    }

    public static class EncodingHelper
    {
        public static rmq.Encoding ToProtobuf(MqEncoding mqEncoding)
        {
            switch (mqEncoding)
            {
                case MqEncoding.Gzip:
                    return rmq.Encoding.Gzip;
                case MqEncoding.Identity:
                    return rmq.Encoding.Identity;
                default:
                    return rmq.Encoding.Unspecified;
            }
        }
    }
}