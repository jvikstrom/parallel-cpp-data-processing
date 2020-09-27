#pragma once

#include "source.hpp"

#include <memory>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <stdio.h>

// Sinks must be thread safe.

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
  std::mutex mtx;
public:
  ~MemorySink() {}
  void write(const Value_Type& value) override {
    std::lock_guard<std::mutex> lk(mtx);
    data.push_back(value);
  }
  std::unique_ptr<Source<Value_Type>> to_source() override {
    std::lock_guard<std::mutex> lk(mtx);
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
  std::mutex mtx;
public:
  ~MemoryKVSink() {}
  void write(const Key_Type& key, const Value_Type& value) override {
    std::lock_guard<std::mutex> lk(mtx);
    data[key].push_back(value);
  }
  std::unique_ptr<KVSource<Key_Type, Value_Type>> to_source() override {
    std::lock_guard<std::mutex> lk(mtx);
    return std::unique_ptr<MemoryKVSource<Key_Type, Value_Type>>(new MemoryKVSource<Key_Type, Value_Type>(data));
    //return std::make_unique(MemoryKVSource<Key_Type, Value_Type>(data));
  }
};

template<typename Key_Type, typename Value_Type>
class KVFileSink : public KVSink<Key_Type, Value_Type> {
  FILE* file;
  std::function<std::string(const Key_Type&, const Value_Type&)> encoder;
public:
  KVFileSink(FILE* file, std::function<std::string(const Key_Type&, const Value_Type&)> encoder) : file(file), encoder(encoder) {}
  ~KVFileSink() {}
  void write(const Key_Type& key, const Value_Type& value) override {
    std::string encoded = encoder(key, value);
    std::size_t sz = encoded.length();
    size_t written = fwrite(reinterpret_cast<const char*>(&sz), sizeof(char), 1, file);
    if(written != sizeof(char)) {
      std::cout << "exception: Did not write length of size_t" << std::endl;
      throw "Did not write length of size_t";
    }
    written = fwrite(encoded.c_str(), sizeof(char), encoded.length(), file);
    if(written != encoded.length()) {
      std::cout << "exception: Did not write the entire encoded key-value pair" << std::endl;
      throw "Did not write the entire encoded key-value pair";
    }
  }
  FILE* get_file() const {
    return file;
  }
  std::unique_ptr<KVSource<Key_Type, Value_Type>> to_source() override {
    std::cout << "exception: Unimplemented" << std::endl;
    throw "Unimplemented";
  }
  std::unique_ptr<KVSource<Key_Type, Value_Type>> to_source(std::size_t buffer_size, std::function<KV<Key_Type, Value_Type>(const std::string&)> decoder) {
    return std::unique_ptr<KVFileSource<Key_Type, Value_Type>>(new KVFileSource<Key_Type, Value_Type>(file, buffer_size, decoder));
  }
};

template<typename Key_Type, typename Value_Type>
class ShardedKVFileSink : public KVSink<Key_Type, Value_Type> {
  std::vector<std::string> shards;
  std::function<std::size_t(const Key_Type&)> hasher;
  std::function<std::string(const Key_Type&, const Value_Type&)> encoder;
  std::vector<KVFileSink<Key_Type, Value_Type>> sinks;
  std::mutex mtx;
  void open_shards() {
    for(const std::string& shard : shards) {
      FILE* f = fopen(shard.c_str(), "w");
      if(!f) {
        std::cout << "exception: Failed opening sharded file: " << shard << std::endl;
        throw "Failed opening sharded file";
      }
      sinks.push_back(KVFileSink<Key_Type, Value_Type>(f, encoder));
    }
  }
public:
  ShardedKVFileSink(const std::vector<std::string>& shards, std::function<std::size_t(const Key_Type&)> hasher, std::function<std::string(const Key_Type&, const Value_Type&)> encoder) : shards(shards), hasher(hasher), encoder(encoder) {
    open_shards();
  }
  ~ShardedKVFileSink() {
    for(const auto& sink : sinks) {
      int closecode = fclose(sink.get_file());
      if(closecode) {
        std::cerr << "Could not close sink with code: " << closecode << std::endl;
      }
    }
  }
  void write(const Key_Type& key, const Value_Type& value) override {
    std::lock_guard<std::mutex> lk(mtx);
    std::size_t shard = hasher(key) % sinks.size();
    sinks[shard].write(key, value);
  }
  std::unique_ptr<ShardedKVFileSource<Key_Type, Value_Type>> to_source(std::size_t buffer_size, std::function<KV<Key_Type, Value_Type>(const std::string&)> decoder) {
    std::lock_guard<std::mutex> lk(mtx);
    return std::unique_ptr<ShardedKVFileSource<Key_Type, Value_Type>>(new ShardedKVFileSource<Key_Type, Value_Type>(shards, buffer_size, decoder));
  }
  std::unique_ptr<KVSource<Key_Type, Value_Type>> to_source() override {
    std::cout << "exception: Unimplemented" << std::endl;
    throw "Unimplemented";
  }

};

} // namespace mr
