#include <iostream>
#include <vector>
#include "map_reduce.hpp"
#include "io/sink.hpp"
#include "io/source.hpp"

int main() {
  std::vector<int> in_data{1,3,6,7,12,20};
  mr::MemorySource<int> src(in_data);
  mr::MemorySink<double> sink;
  // using MapFn = std::function<void(const In_Type&, const Emit<Key_Type, Out_Type>&)>;
  std::mutex mtx;
  mr::MapFn<int,int,int> map_fn = [&](const int& v, const mr::Emit<int,int>& emit_fn) -> void{
    std::unique_lock<std::mutex> lk(mtx);
    std::cout << "(" << std::this_thread::get_id() << ") EMIT: " << v << ": " << v*2 << " and " << v*3 << std::endl;
    emit_fn.emit(v, v*2);
    emit_fn.emit(v, v*3);
  };
  // using ReduceFn = std::function<Out_Type(const Key_Type&, const std::vector<Value_Type>&)>;
  mr::ReduceFn<double, int, int> reduce_fn = [&](const int& key, const std::vector<int>& values) -> double{
    double v = 0.0;
    for(int i : values) {
      v += (double)i + 0.2;
    }
    std::unique_lock<std::mutex> lk(mtx);
    std::cout << "(" << std::this_thread::get_id() << ") REDUCE: " << key << ": " << v << std::endl;
    return v;
  };

  mr::MapReduce<int, int, int, double> mapr(src, sink, map_fn, reduce_fn);
  // std::size_t buffer_size, std::function<std::size_t(const Map_Key_Type &)> hasher, std::function<std::string(const Map_Key_Type &, const Map_Value_Type &)> encoder, std::function<KV<Map_Key_Type, Map_Value_Type>(const std::string &)> decoder
  std::size_t buffer_size = 4096;
  std::function<std::size_t(const int& key)> hasher = [](const int& key) -> std::size_t {return key;};
  std::function<std::string(const int& key, const int& value)> encoder = [](const int& key, const int& value)-> std::string{
    const char* key_c = reinterpret_cast<const char*>(&key);
    const char* value_c = reinterpret_cast<const char*>(&value);
    std::string key_s(key_c, sizeof(int));
    std::string value_s(value_c, sizeof(int));
    return key_s + value_s;
  };
  // std::function<KV<Map_Key_Type, Map_Value_Type>(const std::string &)> decoder
  std::function<mr::KV<int,int>(const std::string&)> decoder = [](const std::string& str) -> mr::KV<int,int>{
    std::string key_s = str.substr(0, sizeof(int));
    std::string value_s = str.substr(sizeof(int));
    int key = *reinterpret_cast<const int*>(key_s.c_str());
    int value = *reinterpret_cast<const int*>(value_s.c_str());
    return mr::KV<int,int>{key, value};
  };
  mapr.run(buffer_size, hasher, encoder, decoder);

  for(double d : sink.get_data()) {
    std::cout << "REDUCED: " << d << std::endl;
  }
}