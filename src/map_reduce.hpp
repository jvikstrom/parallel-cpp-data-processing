#pragma once
#include "io/source.hpp"
#include "internal/map.hpp"
#include "internal/reduce.hpp"
#include "thread/pool.hpp"

namespace mr
{
  namespace
  {
    std::vector<std::string> generate_shards(int n, const std::string &base_name, const std::string &root_dir)
    {
      std::vector<std::string> shards(n);
      for (int i = 0; i < n; i++)
      {
        shards[i] = root_dir + "/" + base_name + std::to_string(i) + "-of-" + std::to_string(n);
      }
      return shards;
    }
  } // namespace

  template <typename In_Type, typename Map_Key_Type, typename Map_Value_Type, typename Out_Type>
  class MapReduce
  {
    Source<In_Type> &src;
    Sink<Out_Type> &sink;
    const MapFn<In_Type, Map_Key_Type, Map_Value_Type> &map_fn;
    const ReduceFn<Out_Type, Map_Key_Type, Map_Value_Type> &reduce_fn;

  public:
    // TODO: Initialize the thread pool to use the number of threads we have cores.
    MapReduce(Source<In_Type> &src, Sink<Out_Type> &sink, const MapFn<In_Type, Map_Key_Type, Map_Value_Type> &map_fn, const ReduceFn<Out_Type, Map_Key_Type, Map_Value_Type> &reduce_fn) : src(src), sink(sink), map_fn(map_fn), reduce_fn(reduce_fn) {}
    void run(std::size_t buffer_size, std::function<std::size_t(const Map_Key_Type &)> hasher, std::function<std::string(const Map_Key_Type &, const Map_Value_Type &)> encoder, std::function<KV<Map_Key_Type, Map_Value_Type>(const std::string &)> decoder)
    {
      // std::function<ShardedKVFileSource::KV(const std::string&)> decoder
      std::vector<std::string> shards = generate_shards(10, "intermediate_kv_", "/home/jovi/Programming/map_reduce_cpp/tmp");
      ShardedKVFileSink<Map_Key_Type, Map_Value_Type> apply_sink(shards, hasher, encoder);
      //MemoryKVSink<Map_Key_Type, Map_Value_Type> apply_sink;
      apply_map(src, apply_sink, map_fn);
      // Remap the sink to a source.
      auto apply_src = apply_sink.to_source(buffer_size, decoder);
      apply_reduce(sink, *apply_src, reduce_fn);
    }
  };
} // namespace mr