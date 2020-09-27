cc_library(
    name = "io",
    srcs = [
    ],
    hdrs = [
        "source.hpp",
        "sink.hpp",
    ],
    deps = [],
    visibility = [
        "//visibility:public",
    ],

)

cc_binary(
    name = "io_test",
    srcs = [
        "streaming_source_test.cc",
    ],
    deps = [
        ":io",
    ],
    copts = [
        "-std=c++2a",
    ]
)
