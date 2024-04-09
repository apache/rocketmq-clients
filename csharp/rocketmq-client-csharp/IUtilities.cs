using System;

namespace Org.Apache.Rocketmq
{
    internal interface IUtilities
    {
        byte[] GetMacAddress();
        int GetProcessId();
        string ByteArrayToHexString(ReadOnlySpan<byte> bytes);
    }
}