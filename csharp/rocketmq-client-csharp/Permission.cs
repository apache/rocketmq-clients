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

using Org.Apache.Rocketmq.Error;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public enum Permission
    {
        None,
        Read,
        Write,
        ReadWrite
    }

    public static class PermissionHelper
    {
        public static Permission FromProtobuf(Proto.Permission permission)
        {
            switch (permission)
            {
                case Proto.Permission.Read:
                    return Permission.Read;
                case Proto.Permission.Write:
                    return Permission.Write;
                case Proto.Permission.ReadWrite:
                    return Permission.ReadWrite;
                case Proto.Permission.None:
                    return Permission.None;
                default:
                    throw new InternalErrorException("Permission is not specified");
            }
        }

        public static Proto.Permission ToProtobuf(Permission permission)
        {
            switch (permission)
            {
                case Permission.Read:
                    return Proto.Permission.Read;
                case Permission.Write:
                    return Proto.Permission.Write;
                case Permission.ReadWrite:
                    return Proto.Permission.ReadWrite;
                default:
                    throw new InternalErrorException("Permission is not specified");
            }
        }

        public static bool IsWritable(Permission permission)
        {
            switch (permission)
            {
                case Permission.Write:
                case Permission.ReadWrite:
                    return true;
                case Permission.None:
                case Permission.Read:
                default:
                    return false;
            }
        }

        public static bool IsReadable(Permission permission)
        {
            switch (permission)
            {
                case Permission.Read:
                case Permission.ReadWrite:
                    return true;
                case Permission.None:
                case Permission.Write:
                default:
                    return false;
            }
        }
    }
}