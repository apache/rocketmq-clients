using Grpc.Core;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{
#if NETFRAMEWORK
    internal static class NetFrameworkExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> kv, out TKey key, out TValue value)
        {
            key = kv.Key;
            value = kv.Value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static IAsyncEnumerable<T> ReadAllAsync<T>(this IAsyncStreamReader<T> streamReader) =>
            new AsyncStreamReaderEnumerable<T>(streamReader);

        private class AsyncStreamReaderEnumerable<T> : IAsyncEnumerable<T>
        {
            private readonly IAsyncStreamReader<T> _streamReader;

            public AsyncStreamReaderEnumerable(IAsyncStreamReader<T> streamReader) => _streamReader = streamReader;

            public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
                new AsyncStreamReaderEnumerator(_streamReader, cancellationToken);

            private class AsyncStreamReaderEnumerator : IAsyncEnumerator<T>
            {
                private readonly IAsyncStreamReader<T> _streamReader;
                private readonly CancellationToken _cancellationToken;

                public AsyncStreamReaderEnumerator(IAsyncStreamReader<T> streamReader, CancellationToken cancellationToken)
                {
                    _streamReader = streamReader;
                    _cancellationToken = cancellationToken;
                }

                public T Current => _streamReader.Current;

                public ValueTask DisposeAsync() => default;

                public ValueTask<bool> MoveNextAsync() => new(_streamReader.MoveNext(_cancellationToken));
            }
        }
    }
#endif
}