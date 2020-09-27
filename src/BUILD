cc_library(
    name = "map_reduce",
    srcs = [],
    hdrs = [
        "map_reduce.hpp",
    ],
    deps = [
        "//src/thread:pool",
        "//src/internal:map",
        "//src/internal:reduce",
        "//src/io:io"
    ]
)

cc_binary(
    name = "main",
    srcs = [
        "main.cc",
    ],
    deps = [
        ":map_reduce",
        "//src/io:io",
    ],
    copts = [
        "-std=c++2a",
        "-g",
    ]
)
