cc_library(
    name = "map_reduce",
    srcs = [],
    hdrs = [
        "map_reduce.hpp",
    ],
    deps = [
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
    ]
)