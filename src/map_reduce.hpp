#pragma once
#include "io/source.hpp"
#include "internal/map.hpp"
#include "internal/reduce.hpp"
#include "thread/pool.hpp"

namespace mr {

template<typename In_Type, typename Map_Key_Type, typename Map_Value_Type, typename Out_Type>
class MapReduce {
  Source<In_Type>& src;
  Sink<Out_Type>& sink;
  const MapFn<In_Type, Map_Key_Type, Map_Value_Type>& map_fn;
  const ReduceFn<Out_Type, Map_Key_Type, Map_Value_Type>& reduce_fn;
public:
  // TODO: Initialize the thread pool to use the number of threads we have cores.
  MapReduce(Source<In_Type>& src, Sink<Out_Type>& sink, const MapFn<In_Type, Map_Key_Type, Map_Value_Type>& map_fn, const ReduceFn<Out_Type, Map_Key_Type, Map_Value_Type>& reduce_fn) : src(src), sink(sink), map_fn(map_fn), reduce_fn(reduce_fn) {}
  void run() {
    MemoryKVSink<Map_Key_Type, Map_Value_Type> apply_sink;
    apply_map(src, apply_sink, map_fn);
    // Remap the sink to a source.
    auto apply_src = apply_sink.to_source();
    apply_reduce(sink, *apply_src, reduce_fn);
  }
};
}