#pragma once

#include <iostream>
#include <unordered_map>
#include <functional>
namespace mr {

template<typename Out_Type>
class Sink {
public:
  virtual void write(const Out_Type& value) = 0;
};

template<typename Value_Type>
class MemorySink : public Sink<Value_Type> {
public:
  std::vector<Value_Type> data;
  void write(const Value_Type& value) override {
    data.push_back(value);
  }
};

template<typename Key_Type, typename Value_Type>
class KVSink {
public:
  virtual void write(const Key_Type& key, const Value_Type& value) = 0;
};

//TODO: Implement sharded file KVSink that hashes the key and places values of the same key in the same file. 

template<typename Key_Type, typename Value_Type>
class MemoryKVSink : public KVSink<Key_Type, Value_Type> {
public:
  std::unordered_map<Key_Type, std::vector<Value_Type>> data;
  void write(const Key_Type& key, const Value_Type& value) override {
    data[key].push_back(value);
  }
};

} // namespace mr
