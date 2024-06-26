"""Load dependencies needed to compile and test the RocketMQ library as a 3rd-party consumer."""

load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def rocketmq_deps():
    """Loads dependencies need to compile and test the RocketMQ library."""
    native.bind(
        name = "opentelementry_api",
        actual = "@com_github_opentelemetry//api:api",
    )

    maybe(
        http_archive,
        name = "com_google_googletest",
        sha256 = "b4870bf121ff7795ba20d20bcdd8627b8e088f2d1dab299a031c1034eddc93d5",
        strip_prefix = "googletest-release-1.11.0",
        urls = [
            "https://github.com/google/googletest/archive/refs/tags/release-1.11.0.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "com_github_gulrak_filesystem",
        strip_prefix = "filesystem-1.5.0",
        sha256 = "eb6f3b0739908ad839cde68885d70e7324db191b9fad63d9915beaa40444d9cb",
        urls = [
            "https://github.com/gulrak/filesystem/archive/v1.5.0.tar.gz",
        ],
        build_file = "@org_apache_rocketmq//third_party:filesystem.BUILD",
    )

    maybe(
        http_archive,
        name = "com_github_gabime_spdlog",
        strip_prefix = "spdlog-1.9.2",
        sha256 = "6fff9215f5cb81760be4cc16d033526d1080427d236e86d70bb02994f85e3d38",
        urls = [
            "https://github.com/gabime/spdlog/archive/refs/tags/v1.9.2.tar.gz",
        ],
        build_file = "@org_apache_rocketmq//third_party:spdlog.BUILD",
    )

    maybe(
        http_archive,
        name = "com_github_fmtlib_fmt",
        strip_prefix = "fmt-8.0.1",
        sha256 = "b06ca3130158c625848f3fb7418f235155a4d389b2abc3a6245fb01cb0eb1e01",
        urls = [
            "https://github.com/fmtlib/fmt/archive/refs/tags/8.0.1.tar.gz",
        ],
        build_file = "@org_apache_rocketmq//third_party:fmtlib.BUILD",
    )

    maybe(
        http_archive,
        name = "com_google_protobuf",
        sha256 = "8b28fdd45bab62d15db232ec404248901842e5340299a57765e48abe8a80d930",
        strip_prefix = "protobuf-3.20.1",
        urls = [
            "https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.20.1.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "rules_proto_grpc",
        sha256 = "507e38c8d95c7efa4f3b1c0595a8e8f139c885cb41a76cab7e20e4e67ae87731",
        strip_prefix = "rules_proto_grpc-4.1.1",
        urls = [
            "https://github.com/rules-proto-grpc/rules_proto_grpc/archive/refs/tags/4.1.1.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "io_opencensus_cpp",
        sha256 = "317f2bfdaba469561c7e64b1a55282b87e677c109c9d8877097940e6d5cbca08",
        urls = [
            "https://github.com/lizhanhui/opencensus-cpp/archive/refs/tags/v0.4.1.tar.gz",
        ],
        strip_prefix = "opencensus-cpp-0.4.1",
    )

    maybe(
        http_archive,
        name = "com_google_absl",
        sha256 = "dcf71b9cba8dc0ca9940c4b316a0c796be8fab42b070bb6b7cab62b48f0e66c4",
        strip_prefix = "abseil-cpp-20211102.0",
        urls = [
            "https://github.com/abseil/abseil-cpp/archive/refs/tags/20211102.0.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "com_github_gflags_gflags",
        strip_prefix = "gflags-2.2.2",
        sha256 = "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf",
        urls = [
            "https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "com_github_grpc_grpc",
        strip_prefix = "grpc-1.46.3",
        sha256 = "d6cbf22cb5007af71b61c6be316a79397469c58c82a942552a62e708bce60964",
        urls = [
            "https://github.com/grpc/grpc/archive/refs/tags/v1.46.3.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "asio",
        sha256 = "c864363205f78768c795ba14a9989200075e732f877ddef01a19237c2eccf44b",
        build_file = "@org_apache_rocketmq//third_party:asio.BUILD",
        strip_prefix = "asio-1.18.2",
        urls = [
            "https://github.com/lizhanhui/asio/archive/refs/tags/v1.18.2.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "com_google_googleapis",
        sha256 = "e89f15d54b0ddab0cd41d18cb2299e5447db704e2b05ff141cb1769170671466",
        urls = [
            "https://github.com/googleapis/googleapis/archive/af7fb72df59a814221b123a4d1acb3f6c3e6cc95.zip",
        ],
        strip_prefix = "googleapis-af7fb72df59a814221b123a4d1acb3f6c3e6cc95",
    )

    maybe(
        http_archive,
        name = "rules_python",
        sha256 = "cdf6b84084aad8f10bf20b46b77cb48d83c319ebe6458a18e9d2cebf57807cdd",
        strip_prefix = "rules_python-0.8.1",
        urls = [
            "https://github.com/bazelbuild/rules_python/archive/refs/tags/0.8.1.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "rules_swift",
        urls = [
            "https://github.com/bazelbuild/rules_swift/archive/refs/tags/0.27.0.tar.gz",
        ],
        strip_prefix = "rules_swift-0.27.0",
    )

    maybe(
        http_archive,
        name = "io_bazel_rules_go",
        sha256 = "685052b498b6ddfe562ca7a97736741d87916fe536623afb7da2824c0211c369",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.33.0/rules_go-v0.33.0.zip",
            "https://github.com/bazelbuild/rules_go/releases/download/v0.33.0/rules_go-v0.33.0.zip",
        ],
    )

    maybe(
        http_archive,
        name = "rules_proto",
        sha256 = "e017528fd1c91c5a33f15493e3a398181a9e821a804eb7ff5acdd1d2d6c2b18d",
        strip_prefix = "rules_proto-4.0.0-3.20.0",
        urls = [
            "https://github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0-3.20.0.tar.gz",
        ],
    )

    maybe(
        http_archive,
        name = "com_github_opentelemetry",
        strip_prefix = "opentelemetry-cpp-1.14.2",
        urls = [
            "https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v1.14.2.tar.gz"
        ]
    )
