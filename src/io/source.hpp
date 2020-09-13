#pragma once

#include <unordered_map>
#include <functional>
namespace mr {

template<typename T>
class Source {
public:
  virtual T next() = 0;
  virtual bool has_next() = 0;
};

template<typename T>
class MemorySource : public Source<T>{
  std::size_t i = 0;
public:
  std::vector<T> data;
  MemorySource(const std::vector<T>& in) : data(in) {}
  T next() override {
    return data.at(i++);
  }
  bool has_next() override {
    return i < data.size();
  }
};

template<typename Key_Type, typename Value_Type>
class KVSource {
public:
  virtual std::pair<Key_Type, std::vector<Value_Type>> next() = 0;
  virtual bool has_next() = 0;
};

template<typename Key_Type, typename Value_Type>
class MemoryKVSource : public KVSource<Key_Type, Value_Type> {
  typename std::unordered_map<Key_Type, std::vector<Value_Type>>::iterator data_it;
public:
  std::unordered_map<Key_Type, std::vector<Value_Type>> data;
  MemoryKVSource(const std::unordered_map<Key_Type, std::vector<Value_Type>>& data) : data(data) {
    data_it = this->data.begin();
  }
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
