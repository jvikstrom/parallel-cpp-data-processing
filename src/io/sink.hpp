#pragma once

#include "source.hpp"

#include <memory>
#include <unordered_map>
#include <functional>
namespace mr {

template<typename Out_Type>
class Sink {
public:
  virtual ~Sink() {}
  virtual void write(const Out_Type& value) = 0;
  virtual std::unique_ptr<Source<Out_Type>> to_source() = 0;
};

template<typename Value_Type>
class MemorySink : public Sink<Value_Type> {
  std::vector<Value_Type> data;
public:
  ~MemorySink() {}
  void write(const Value_Type& value) override {
    data.push_back(value);
  }
  std::unique_ptr<Source<Value_Type>> to_source() override {
    return std::unique_ptr<MemorySource<Value_Type>>(new MemorySource<Value_Type>(data));
    //return std::make_unique<MemorySource<Value_Type>>(data);
    //return std::make_unique<MemorySource<typename Value_Type>>(data);
  }
  const std::vector<Value_Type>& get_data() {
    return data;
  }
};

template<typename Key_Type, typename Value_Type>
class KVSink {
public:
  virtual ~KVSink() {}
  virtual void write(const Key_Type& key, const Value_Type& value) = 0;
  virtual std::unique_ptr<KVSource<Key_Type, Value_Type>> to_source() = 0;
};

//TODO: Implement sharded file KVSink that hashes the key and places values of the same key in the same file. 

template<typename Key_Type, typename Value_Type>
class MemoryKVSink : public KVSink<Key_Type, Value_Type> {
  std::unordered_map<Key_Type, std::vector<Value_Type>> data;
public:
  ~MemoryKVSink() {}
  void write(const Key_Type& key, const Value_Type& value) override {
    data[key].push_back(value);
  }
  std::unique_ptr<KVSource<Key_Type, Value_Type>> to_source() override {
    return std::unique_ptr<MemoryKVSource<Key_Type, Value_Type>>(new MemoryKVSource<Key_Type, Value_Type>(data));
    //return std::make_unique(MemoryKVSource<Key_Type, Value_Type>(data));
  }
};

} // namespace mr
