#pragma once
#include "src/io/source.hpp"
#include "src/io/sink.hpp"
#include "src/thread/pool.hpp"

namespace mr {

template<typename Out_Type, typename Key_Type, typename Value_Type>
using ReduceFn = std::function<Out_Type(const Key_Type&, const std::vector<Value_Type>&)>;

template<typename Key_Type, typename Value_Type, typename Out_Type>
void apply_reduce(Sink<Out_Type>& sink, KVSource<Key_Type, Value_Type>& src, const ReduceFn<Out_Type, Key_Type, Value_Type>& reduce_fn) {
  thread::Pool pool(4);
  std::mutex m;
  while(src.has_next()) {
    const auto& value = src.next();
    pool.add_job([&]() ->void{
      const Out_Type& out = reduce_fn(value.first, value.second);
      sink.write(out);
    });
  }
}
}

