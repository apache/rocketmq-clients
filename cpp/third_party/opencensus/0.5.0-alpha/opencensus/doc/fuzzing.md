# Fuzzing

To build the fuzzers and run them in test only mode:

```shell
rm -rf .build
tools/fuzz.sh
```

To run the fuzzer and find bugs:

```shell
mkdir /tmp/fuzz
./.build/opencensus/trace/opencensus_trace_cloud_trace_context_fuzzer \
  /tmp/fuzz opencensus/trace/internal/cloud_trace_context_corpus
```

The corpus directories are not meant to give complete coverage, but
rather should only contain manually generated inputs that serve as a seed
for the fuzzer, so it can more quickly find a useful search space.
