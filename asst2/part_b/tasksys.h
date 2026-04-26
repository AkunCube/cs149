#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char *name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  struct Task {
    IRunnable *runnable = nullptr;
    TaskID id;
    int num_total_tasks;
    int next_task;
    std::atomic<int> num_completed;

    Task(IRunnable *runnable, TaskID id, int num_total_tasks)
        : runnable(runnable), id(id), num_total_tasks(num_total_tasks),
          next_task(0), num_completed(0) {}
  };

  int num_threads;
  std::vector<std::thread> workers;
  std::mutex mutex;
  bool shutdown;
  std::condition_variable work_cv;
  std::condition_variable done_cv;

  TaskID next_task_id;
  std::unordered_map<TaskID, std::unordered_set<TaskID>> forward_graph;
  std::unordered_map<TaskID, std::unique_ptr<Task>> task_map;
  std::unordered_map<TaskID, int> num_deps;

  std::list<Task *> executable_tasks;

  void workerLoop();
};

#endif
