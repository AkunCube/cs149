#include "tasksys.h"
#include <algorithm>
#include <memory>
#include <mutex>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemSerial::sync() { return; }

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable,
                                               int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // NOTE: CS149 students are not expected to implement
  // TaskSystemParallelThreadPoolSpinning in Part B.
  return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads), shutdown(false),
      next_task_id(0) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  workers.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop,
                         this);
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  {
    std::lock_guard<std::mutex> lock(mutex);
    shutdown = true;
  }
  work_cv.notify_all();

  for (auto &worker : workers) {
    worker.join();
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {

  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
  // Main worker thread loop, runs until shutdown is triggered.
  while (true) {
    Task *task = nullptr;
    int batch_id = 0;
    int total_tasks = 0;
    TaskID task_id = -1;
    IRunnable *runnable = nullptr;
    auto it = executable_tasks.end();
    {
      // Minimize lock scope: only for task acquisition.
      std::unique_lock<std::mutex> lock(mutex);
      // Wait until shutdown is signaled or there are executable tasks.
      work_cv.wait(lock,
                   [this]() { return shutdown || !executable_tasks.empty(); });

      // Exit the loop if the system is shutting down.
      if (shutdown) {
        break;
      }

      // Find the first task that still has unassigned batches.
      it = std::find_if(
          executable_tasks.begin(), executable_tasks.end(),
          [](Task *t) { return t->next_task < t->num_total_tasks; });

      // No assignable task found, restart the loop.
      if (it == executable_tasks.end()) {
        continue;
      }

      // Extract task parameters and atomically assign a batch ID.
      task = *it;
      // Cache id locally; cleanup below erases task_map[id], destroying *task.
      task_id = task->id;
      runnable = task->runnable;
      total_tasks = task->num_total_tasks;
      batch_id = task->next_task++;
    }

    // Execute the task outside the lock to avoid long lock hold time.
    runnable->runTask(batch_id, total_tasks);

    bool need_wake_worker = false;
    bool need_wake_done = false;

    // Increment completed task count and check if the task is fully done.
    const int completed = ++task->num_completed;
    if (completed == task->num_total_tasks) {
      std::lock_guard<std::mutex> lock(mutex);

      // Process child dependencies: topological sort activation.
      auto fg_it = forward_graph.find(task_id);
      if (fg_it != forward_graph.end()) {
        for (const TaskID &child_id : fg_it->second) {
          if (--num_deps[child_id] == 0) {
            executable_tasks.push_back(task_map[child_id].get());
            need_wake_worker = true;
          }
        }
        forward_graph.erase(fg_it);
      }

      // Clean up completed task resources.
      executable_tasks.erase(it);
      task_map.erase(task_id);

      // Notify done if no executable tasks remain.
      if (executable_tasks.empty()) {
        need_wake_done = true;
      }
    }

    // Wake one worker to process new tasks, avoids thundering herd.
    if (need_wake_worker) {
      work_cv.notify_all();
    }

    // Notify all waiters that all tasks are completed.
    if (need_wake_done) {
      done_cv.notify_all();
    }
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
  //
  // TODO: CS149 students will implement this method in Part B.
  //
  bool can_run = true;
  {
    std::lock_guard<std::mutex> g(mutex);
    for (const TaskID &dep : deps) {
      // Since worker can remove finish tasks from task_map, so we should't add
      // it to forward_graph if it is done.
      if (!task_map.count(dep)) {
        continue;
      }
      if (forward_graph[dep].insert(next_task_id).second) {
        num_deps[next_task_id]++;
        can_run = false; // This task has a dependency, so it is not ready.
      }
    }

    std::unique_ptr<Task> task =
        std::make_unique<Task>(runnable, next_task_id, num_total_tasks);
    if (can_run) {
      executable_tasks.push_back(task.get());
    }
    task_map.emplace(next_task_id, std::move(task));
  }

  if (can_run) {
    work_cv.notify_all();
  }

  return next_task_id++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //
  std::unique_lock<std::mutex> lock(mutex);
  done_cv.wait(lock, [this]() {
    // Wake up when all tasks are done.
    return executable_tasks.empty();
  });
  return;
}
