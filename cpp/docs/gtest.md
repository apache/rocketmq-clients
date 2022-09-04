# GoogleTest Quickstart

## Run a single test case

1. Get all test cases by running tests with --gtest_list_tests
2. Parse this data into your GUI
3. Select test cases you want ro run
4. Run test executable with option --gtest_filter=

## Run tests multiple times

bazel test --runs_per_test=10 //...
