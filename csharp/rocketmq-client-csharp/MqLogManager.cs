using System;
using System.IO;
using System.Reflection;
using NLog;
using NLog.Config;

namespace Org.Apache.Rocketmq
{
    /**
     * RocketMQ Log Manager.
     *
     * Configure component logging, please refer to https://github.com/NLog/NLog/wiki/Configure-component-logging
     */
    public class MqLogManager
    {
        public static LogFactory Instance
        {
            get { return LazyInstance.Value; }
        }

        private static readonly Lazy<LogFactory> LazyInstance = new(BuildLogFactory);

        private static LogFactory BuildLogFactory()
        {
            // Use name of current assembly to construct NLog config filename 
            Assembly thisAssembly = Assembly.GetExecutingAssembly();
            string configFilePath = Path.ChangeExtension(thisAssembly.Location, ".nlog");

            LogFactory logFactory = new LogFactory();
            logFactory.Configuration = new XmlLoggingConfiguration(configFilePath, logFactory);
            return logFactory;
        }
    }
}