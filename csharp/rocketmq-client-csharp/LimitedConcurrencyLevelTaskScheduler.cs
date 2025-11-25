using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{
    public class LimitedConcurrencyLevelTaskScheduler : TaskScheduler
    {
        private static bool _currentThreadIsProcessingItems;
        private readonly LinkedList<Task> _tasks = new();
        private readonly int _maxDegreeOfParallelism;
        private int _delegatesQueuedOrRunning;

        public sealed override int MaximumConcurrencyLevel
        {
            get
            {
                return _maxDegreeOfParallelism;
            }
        }
        public LimitedConcurrencyLevelTaskScheduler(int maxDegreeOfParallelism)
        {
            if (maxDegreeOfParallelism >= 1)
            {
                _maxDegreeOfParallelism = maxDegreeOfParallelism;
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(maxDegreeOfParallelism));
            }
        }
        protected sealed override void QueueTask(Task task)
        {
            lock (_tasks)
            {
                _tasks.AddLast(task);
                if (_delegatesQueuedOrRunning < _maxDegreeOfParallelism)
                {
                    _delegatesQueuedOrRunning++;
                    NotifyThreadPoolOfPendingWork();
                }
            }
        }
        private void NotifyThreadPoolOfPendingWork()
        {
            ThreadPool.UnsafeQueueUserWorkItem(delegate (object _)
            {
                _currentThreadIsProcessingItems = true;
                try
                {
                    while (true)
                    {
                        Task value;
                        lock (_tasks)
                        {
                            if (_tasks.Count == 0)
                            {
                                _delegatesQueuedOrRunning--;
                                break;
                            }
                            value = _tasks.First.Value;
                            _tasks.RemoveFirst();
                        }
                        base.TryExecuteTask(value);
                    }
                }
                finally
                {
                    _currentThreadIsProcessingItems = false;
                }
            }, null);
        }
        protected sealed override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            if (!_currentThreadIsProcessingItems)
            {
                return false;
            }
            if (taskWasPreviouslyQueued)
            {
                TryDequeue(task);
            }
            return base.TryExecuteTask(task);
        }
        protected sealed override bool TryDequeue(Task task)
        {
            bool result;
            lock (_tasks)
            {
                result = _tasks.Remove(task);
            }
            return result;
        }
        protected sealed override IEnumerable<Task> GetScheduledTasks()
        {
            bool flag = false;
            IEnumerable<Task> result;
            try
            {
                Monitor.TryEnter(_tasks, ref flag);
                if (!flag)
                {
                    throw new NotSupportedException();
                }
                result = _tasks.ToArray();
            }
            finally
            {
                if (flag)
                {
                    Monitor.Exit(_tasks);
                }
            }
            return result;
        }
    }
}
