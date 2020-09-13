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
  mr::MapFn<int,int,int> map_fn = [](const int& v, const mr::Emit<int,int>& emit_fn) -> void{
    emit_fn.emit(v, v*2);
    emit_fn.emit(v, v*3);
  };
  // using ReduceFn = std::function<Out_Type(const Key_Type&, const std::vector<Value_Type>&)>;
  mr::ReduceFn<double, int, int> reduce_fn = [](const int& key, const std::vector<int>& values) -> double{
    double v = 0.0;
    for(int i : values) {
      v += (double)i + 0.2;
    }
    return v;
  };

  mr::MapReduce<int, int, int, double> mapr(src, sink, map_fn, reduce_fn);
  mapr.run();

  for(double d : sink.data) {
    std::cout << "REDUCED: " << d << std::endl;
  }
}