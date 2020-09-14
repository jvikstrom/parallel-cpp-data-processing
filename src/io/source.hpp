#pragma once

#include <unordered_map>
#include <functional>
#include <vector>
#include <stdio.h>
#include <iostream>
namespace mr {

template<typename T>
class Source {
public:
  virtual ~Source() {}
  virtual T next() = 0;
  virtual bool has_next() = 0;
};

template<typename T>
class MemorySource : public Source<T>{
  std::size_t i = 0;
  std::vector<T> data;
public:
  MemorySource(const std::vector<T>& in) : data(in) {}
  virtual ~MemorySource() {}
  T next() override {
    return data.at(i++);
  }
  bool has_next() override {
    return i < data.size();
  }
};

template<typename T>
class StreamingFileSource : public Source<T> {
  // Will read a maximum of 2 * buffer_size memory for reading from the file (unless there's a record that's larger than that size).
  std::function<T(const std::string&)> decoder;
  FILE* file = nullptr;
  const std::size_t buffer_size;

  std::string buffer;

  int i = 0;
  // Returns false if we're at eof.
  bool read_next_chunk(std::string& buf) {
    buf.resize(buffer_size);
    i = 0;
    std::size_t n_read = fread(&buf[0], sizeof(char), buffer_size, file);
    buf.resize(n_read);
    return n_read != 0;
  }
  std::string read_bytes(std::size_t n) {
    // Assumes the file contains n number of bytes.
    // Reads n bytes from the buffer at the current position.
    std::string data;
    data.resize(n);
    for(int j = 0; j < n; j++) {
      if(i >= buffer.size()) {
        // Discard the eof flag. We never hit that here.
        read_next_chunk(buffer);
      }
      data[j] = buffer[i++];
    }
    return data;
  }
  std::size_t decode_size_t(const std::string& in) {
    return *reinterpret_cast<const std::size_t*>(in.data());
  }
public:
  StreamingFileSource(FILE* file, std::size_t buffer_size, std::function<T(const std::string&)> decoder) : decoder(decoder), file(file), buffer_size(buffer_size) {
    buffer.resize(buffer_size);
    read_next_chunk(buffer);
  }
  virtual ~StreamingFileSource() {
    fclose(file);
  }
  bool has_next() override {
    if(i >= buffer.size()) {
      // Need to read the next chunk.
      i = 0;
      bool ret = read_next_chunk(buffer);
      return ret;
    }
    return true;
  }
  T next() override {
    std::size_t next_size = decode_size_t(read_bytes(sizeof(std::size_t)));
    std::string next_val = read_bytes(next_size);
    return decoder(next_val);
  }

};

template<typename Key_Type, typename Value_Type>
class KVSource {
public:
  virtual ~KVSource() {}
  virtual std::pair<Key_Type, std::vector<Value_Type>> next() = 0;
  virtual bool has_next() = 0;
};

template<typename Key_Type, typename Value_Type>
class MemoryKVSource : public KVSource<Key_Type, Value_Type> {
  typename std::unordered_map<Key_Type, std::vector<Value_Type>>::iterator data_it;
  std::unordered_map<Key_Type, std::vector<Value_Type>> data;
public:
  MemoryKVSource(const std::unordered_map<Key_Type, std::vector<Value_Type>>& data) : data(data) {
    data_it = this->data.begin();
  }
  virtual ~MemoryKVSource() {}
  std::pair<Key_Type, std::vector<Value_Type>> next() override {
    auto data = *data_it;
    ++data_it;
    return data;
  }
  bool has_next() override {
    return data_it != data.end();
  }
};

} // namespace mr
