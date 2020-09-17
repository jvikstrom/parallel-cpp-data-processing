#pragma once
#include <thread>
#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace mr {
namespace thread {
class Pool {
// A thread pool.
  std::mutex job_queue_lock;
  std::condition_variable queue_var;
  std::atomic<bool> should_quit;
  std::queue<std::function<void()>> jobs;
  std::vector<std::thread> threads;
  void run_worker();
public:
  Pool(int n_threads);
  // Wait until the thread pool is empty of jobs and join the threads.
  ~Pool();
  void add_job(std::function<void()> f);
};
}
}