using System;

namespace Org.Apache.Rocketmq
{
    internal class DefaultUtilities : IUtilities
    {
        private DefaultUtilities()
        {
        }

        public static DefaultUtilities Instance { get; } = new DefaultUtilities();

        public byte[] GetMacAddress()
        {
            return Utilities.GetMacAddress();
        }

        public int GetProcessId()
        {
            return Utilities.GetProcessId();
        }

        public string ByteArrayToHexString(ReadOnlySpan<byte> bytes)
        {
            return Utilities.ByteArrayToHexString(bytes);
        }
    }
}