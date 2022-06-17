load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "cpp_httplib",
    hdrs = [
        "httplib.h",
    ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "//external:madler_zlib",
    ],
    defines = [
        "CPPHTTPLIB_ZLIB_SUPPORT",
    ],
)