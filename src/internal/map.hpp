#pragma once
#include "src/io/source.hpp"
#include "src/io/sink.hpp"
#include "src/thread/pool.hpp"

namespace mr {

template<typename Key_Type, typename Value_Type>
class Emit {
public:
  virtual void emit(const Key_Type& key, const Value_Type& value) const = 0;
};

template<typename In_Type, typename Key_Type, typename Out_Type>
using MapFn = std::function<void(const In_Type&, const Emit<Key_Type, Out_Type>&)>;

// Collects all output from a single thread into a file by appending.
template<typename Key_Type, typename Value_Type>
class SingleThreadEmitCollector : public Emit<Key_Type, Value_Type>{
  KVSink<Key_Type, Value_Type>& sink;
public:
  SingleThreadEmitCollector(KVSink<Key_Type, Value_Type>& sink) : sink(sink) {}
  void emit(const Key_Type& key, const Value_Type& value) const override {
    // Write t to a sink.
    sink.write(key, value);
  }
};

// Apply the map operation to a source.
// TODO: Pass in a thread-pool here.
template<typename In_Type, typename Key_Type, typename Out_Type>
void apply_map(Source<In_Type>& src, KVSink<Key_Type, Out_Type>& sink, MapFn<In_Type, Key_Type, Out_Type> map_fn) {
  thread::Pool pool(4);
  // TODO: Run this over multiple threads.
  SingleThreadEmitCollector<Key_Type, In_Type> emit_collector(sink);
  while(src.has_next()) {
    const In_Type& value = src.next();
    pool.add_job([map_fn, value, emit_collector]() ->void{
      map_fn(value, emit_collector);
    });
  }
  
}
}