# Benchmarks

## Running

Benchmarks are implemented as binary rules in bazel, and can be either built and
then run from the binary location in `bazel-bin` or run with `bazel run`, e.g.
```shell
bazel run -c opt opencensus/stats:stats_manager_benchmark [-- BENCHMARK_FLAGS]
```
Benchmarks use the [Google benchmark](https://github.com/google/benchmark)
library. This accepts several helpful flags, including
 - --benchmark_filter=REGEX: Run only benchmarks whose names match REGEX.
 - --benchmark_repetitions=N: Repeat each benchmark and calculate
   mean/median/stddev.
 - --benchmark_report_aggregates_only={true|false}: In conjunction with
   benchmark_repetitions, report only summary statistics and not single-run
   timings.

## Profiling

Benchmarks can be profiled using the
[gperftools](https://github.com/gperftools/gperftools) library. On
Debian/Ubuntu, install the `google-perftools` (profile analysis tools) and
`libgoogle-perftools-dev` (profiling library) packages. When running the
benchmark, set `LD_PRELOAD=/usr/lib/libprofiler.so` to enable the profiler and
`CPUPROFILE=PATH` to save a profile, and analyze the profile with
`google-pprof` (which needs a path to the binary for symbolization). For
example,
```shell
bazel build -c opt opencensus/stats:stats_manager_benchmark
LD_PRELOAD=/usr/lib/libprofiler.so CPUPROFILE=/tmp/prof \
    ./bazel-bin/opencensus/stats/stats_manager_benchmark
google-pprof --web bazel-bin/opencensus/stats/stats_manager_benchmark /tmp/prof
```
pprof supports many analysis types--see its documentation for options.
