#pragma once
#include "sink.hpp"
#include "source.hpp"

namespace mr {

template<typename T>
MemorySink<T> memory_source_to_sink(const MemorySource<T>& src) {
  return MemorySink<T>(src.data);
}

template<typename T>
MemorySource<T> memory_sink_to_source(const MemorySink<T>& sink) {
  return MemorySource<T>(sink.data);
}

template<typename Key_Type, typename Value_Type>
MemoryKVSource<Key_Type, Value_Type> memory_kv_sink_to_source(const MemoryKVSink<Key_Type, Value_Type>& sink) {
  return MemoryKVSource<Key_Type, Value_Type>(sink.data);
}

template<typename Key_Type, typename Value_Type>
MemoryKVSink<Key_Type, Value_Type> memory_kv_source_to_sink(const MemoryKVSource<Key_Type, Value_Type>& src) {
  return MemoryKVSink<Key_Type, Value_Type>(src.data);
}


} // namespace mr
