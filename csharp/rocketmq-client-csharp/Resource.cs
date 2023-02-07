using System;
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class Resource
    {
        public Resource(Proto.Resource resource)
        {
            Namespace = resource.ResourceNamespace;
            Name = resource.Name;
        }

        public string Namespace { get; }
        public string Name { get; }

        public Proto.Resource ToProtobuf()
        {
            return new Proto.Resource
            {
                ResourceNamespace = Namespace,
                Name = Name
            };
        }
        
        public override string ToString()
        {
            return String.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
        }
    }
}