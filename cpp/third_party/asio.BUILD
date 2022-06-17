load("@rules_cc//cc:defs.bzl", "cc_library")
cc_library(
    name = "asio",
    hdrs = glob(["include/**/*.hpp", "include/**/*.ipp"]),
    visibility =  ["//visibility:public"],
    includes = [
        "include",
    ],
    defines = [
        "ASIO_STANDALONE",
        "ASIO_HAS_STD_ADDRESSOF",
        "ASIO_HAS_STD_ARRAY",
        "ASIO_HAS_CSTDINT",
        "ASIO_HAS_STD_SHARED_PTR",
        "ASIO_HAS_STD_TYPE_TRAITS",
    ],
)