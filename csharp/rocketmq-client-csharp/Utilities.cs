/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Threading;

namespace Org.Apache.Rocketmq
{
    public static class Utilities
    {
        private static long _instanceSequence = 0;
        private static readonly int ProcessId = Process.GetCurrentProcess().Id;
        private static readonly string HostName = System.Net.Dns.GetHostName();
        public const int MasterBrokerId = 0;

        public static int GetPositiveMod(int k, int n)
        {
            var result = k % n;
            return result < 0 ? result + n : result;
        }

        public static byte[] GetMacAddress()
        {
            return NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault(nic =>
                nic.OperationalStatus == OperationalStatus.Up &&
                nic.NetworkInterfaceType != NetworkInterfaceType.Loopback)?.GetPhysicalAddress().GetAddressBytes();
        }

        public static int GetProcessId()
        {
            return ProcessId;
        }

        public static string GetHostName()
        {
            return HostName;
        }

        public static string GetClientId()
        {
            var hostName = System.Net.Dns.GetHostName();
            var pid = Process.GetCurrentProcess().Id;
            var index = Interlocked.Increment(ref _instanceSequence);
            var nowMillisecond = (long)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalMilliseconds;
            var no = DecimalToBase36(nowMillisecond);
            return $"{hostName}@{pid}@{index}@{no}";
        }

        public static string ComputeMd5Hash(byte[] data)
        {
            using (var md5 = MD5.Create())
            {
                var hashBytes = md5.ComputeHash(data);
                return BitConverter.ToString(hashBytes).Replace("-", "");
            }
        }

        public static string ComputeSha1Hash(byte[] data)
        {
            using (var sha1 = SHA1.Create())
            {
                var hashBytes = sha1.ComputeHash(data);
                return BitConverter.ToString(hashBytes).Replace("-", "");
            }
        }


        private static string DecimalToBase36(long decimalNumber)
        {
            const string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            string result = string.Empty;

            while (decimalNumber > 0)
            {
                result = chars[(int)(decimalNumber % 36)] + result;
                decimalNumber /= 36;
            }

            return result;
        }

        public static string ByteArrayToHexString(byte[] bytes)
        {
            var result = new StringBuilder(bytes.Length * 2);
            const string hexAlphabet = "0123456789ABCDEF";

            foreach (var b in bytes)
            {
                result.Append(hexAlphabet[(int)(b >> 4)]);
                result.Append(hexAlphabet[(int)(b & 0xF)]);
            }

            return result.ToString();
        }

        public static byte[] CompressBytesGzip(byte[] src, CompressionLevel level)
        {
            using (var ms = new MemoryStream())
            {
                using (var gzip = new GZipStream(ms, level))
                {
                    gzip.Write(src, 0, src.Length);
                }

                return ms.ToArray();
            }
        }

        public static byte[] DecompressBytesGzip(byte[] src)
        {
            var inputStream = new MemoryStream(src);
            var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
            var outputStream = new MemoryStream();
            gzipStream.CopyTo(outputStream);
            return outputStream.ToArray();
        }
    }
}