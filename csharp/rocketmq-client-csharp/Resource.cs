using System;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public class Resource
    {
        public Resource(rmq.Resource resource)
        {
            Namespace = resource.ResourceNamespace;
            Name = resource.Name;
        }

        public string Namespace { get; }
        public string Name { get; }

        public rmq.Resource ToProtobuf()
        {
            return new rmq.Resource
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