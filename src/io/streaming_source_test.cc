#include "source.hpp"
#include <stdio.h>
#include <iostream>

namespace {
template<typename POD>
int write_pod(FILE* f, POD const& pod) {
    static_assert(std::is_pod<POD>::value, "must be plain old data");
    return fwrite(reinterpret_cast<char const*>(&pod), sizeof(POD), 1, f);
}

bool test_streaming_source() {
  std::size_t file_size;
  char* file_buf;
  FILE* f = open_memstream(&file_buf, &file_size);
  if(f == nullptr) {
    std::cout << "Could not open file" << std::endl;
    return false;
  }
  write_pod<std::size_t>(f, sizeof(int));
  write_pod<int>(f, 3);
  write_pod<std::size_t>(f, sizeof(int));
  write_pod<int>(f, 4);

  mr::StreamingFileSource<int> streaming_source(f, sizeof(int), [](const std::string& in) ->int{
    return *reinterpret_cast<const int*>(in.data());
  });
  if(!streaming_source.has_next()) {
    return false;
  }
  int nxt = streaming_source.next();
  if(nxt != 3) {
    return false;
  }
  if(!streaming_source.has_next()) {
    return false;
  }
  nxt = streaming_source.next();
  if(nxt != 4) {
    return false;
  }
  std::cout << "Success" << std::endl;
  return true;
}
}

int main() {
  bool ret = test_streaming_source();
  if(!ret) {
    std::cout << "Streaming source failed!" << std::endl;
    return -1;
  }
}
