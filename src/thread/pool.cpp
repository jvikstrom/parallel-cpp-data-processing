#include "pool.hpp"
#include <iostream>
#include <chrono>

namespace mr {
namespace thread {
Pool::Pool(int n_threads) {
// std::thread t(&Pool::run_worker, this);
  for(int i = 0; i < n_threads; i++) {
    threads.emplace_back(&Pool::run_worker, this);
  }
}

Pool::~Pool() {
  should_quit.store(true);
  for(std::thread& t : threads) {
    t.join();
  }
}

void Pool::run_worker() {
  while(!jobs.empty() || !this->should_quit.load()) {
    // TODO: Add predicate that checks if the thread should be shut down.
    std::unique_lock<std::mutex> lk(job_queue_lock);
    if(jobs.empty()) {
      queue_var.wait_for(lk, std::chrono::milliseconds(100));
    }
    if(!lk.owns_lock()) {
      continue;
    }
    
    // There's a job available! Take it and run!
    if(jobs.empty()) {
      // Someone else took the job.
      return;
    }
    auto job = std::move(jobs.front());
    jobs.pop();
    lk.unlock();
    job();
  }
}

void Pool::add_job(std::function<void()> f) {
  {
    std::lock_guard<std::mutex> lk(job_queue_lock);
    jobs.push(f);
  }
  queue_var.notify_one();
}
}
}