# Test Covergge Report

## Generate Coverage Data

```text
    bazel coverage -s           \
    --instrument_test_targets   \
    --experimental_cc_coverage  \ 
    --combined_report=lcov      \
    --coverage_report_generator=@bazel_tools//tools/test/CoverageOutputGenerator/java/com/google/devtools/coverageoutputgenerator:Main  \
   //src/test/...
```

## Generate HTML pages

```text
genhtml bazel-out/_coverage/_coverage_report.dat  \
        --output-directory coverage_html
```
