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

Don't forget to use `-c opt` to get an optimized build.

## Profiling

Benchmarks can be profiled using the
[gperftools](https://github.com/gperftools/gperftools) library. To install on
Debian / Ubuntu:

```shell
sudo apt install google-perftools libgoogle-perftools-dev
```

When running the benchmark, set:
 - `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libprofiler.so.0` to enable the
   profiler.
 - `CPUPROFILE=path_to_output_file`
 - Optional: `CPUPROFILE_FREQUENCY=4000` to increase resolution.
 - Optional: for visibility into inlined functions, build with `-g1` or above.

Analyze the profile with `google-pprof`, which needs a path to the binary for
symbolization.

Example:

```shell
bazel build -c opt --copt=-g1 opencensus/stats:stats_manager_benchmark
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libprofiler.so.0 \
  CPUPROFILE=/tmp/prof \
  CPUPROFILE_FREQUENCY=4000 \
  ./bazel-bin/opencensus/stats/stats_manager_benchmark
google-pprof --web bazel-bin/opencensus/stats/stats_manager_benchmark /tmp/prof
```

`google-pprof` supports many analysis types - see its
[documentation](https://gperftools.github.io/gperftools/cpuprofile.html) for
options.

Note that `CPUPROFILE_FREQUENCY` defaults to 100 samples per second.
The maximum value `libprofiler` will accept is 4000.
Most Linux kernels are built with `CONFIG_HZ=250`, so a single-threaded
benchmark will produce at most 250 samples per second.
