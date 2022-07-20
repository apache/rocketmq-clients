using System;
using System.Threading.Tasks;
using System.Threading;

namespace examples
{

    class Foo
    {
        public int bar = 1;
    }
    class Program
    {

        static void RT(Action action, int seconds, CancellationToken token)
        {
            if (null == action)
            {
                return;
            }

            Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    action();
                    await Task.Delay(TimeSpan.FromSeconds(seconds), token);
                }
            });
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            string accessKey = "key";
            string accessSecret = "secret";
            var credentials = new Org.Apache.Rocketmq.StaticCredentialsProvider(accessKey, accessSecret).getCredentials();
            bool expired = credentials.expired();

            int workerThreads;
            int completionPortThreads;
            ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
            Console.WriteLine($"Max: workerThread={workerThreads}, completionPortThreads={completionPortThreads}");
            ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
            Console.WriteLine($"Min: workerThread={workerThreads}, completionPortThreads={completionPortThreads}");

            ThreadPool.QueueUserWorkItem((Object stateInfo) =>
            {
                Console.WriteLine("From ThreadPool");
                if (stateInfo is Foo)
                {
                    Console.WriteLine("Foo: bar=" + (stateInfo as Foo).bar);
                }
            }, new Foo());

            var cts = new CancellationTokenSource();
            RT(() =>
            {
                Console.WriteLine("Hello Again" + Thread.CurrentThread.Name);
            }, 1, cts.Token);
            cts.CancelAfter(3000);
            Console.ReadKey();
        }
    }
}
