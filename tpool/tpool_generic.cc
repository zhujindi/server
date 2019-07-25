/* Copyright(C) 2019 MariaDB Corporation.

This program is free software; you can redistribute itand /or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02111 - 1301 USA*/

#include "tpool_structs.h"
#include <limits.h>
#include <algorithm>
#include <assert.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <limits.h>
#include <mutex>
#include <queue>
#include <stack>
#include <thread>
#include <vector>
#include "tpool.h"
#include <assert.h>


namespace tpool
{

#ifdef __linux__
  extern aio* create_linux_aio(thread_pool* tp, int max_io);
#endif
#ifdef _WIN32
  extern aio* create_win_aio(thread_pool* tp, int max_io);
#endif


/**
  Implementation of generic threadpool.
  This threadpool consists of the following components

  - The task queue. This queue is populated by submit()
  - Worker that execute the  work items.
  - Timer thread that takes care of pool health
 
  The task queue is populated by submit() method.
  on submit(), a worker thread  can be woken, or created
  to execute tasks.

  The timer thread watches if work items  are being dequeued, and if not, 
  this can indicate potential deadlock.
  Thus the timer thread can also wake or create a thread, to ensure some progress.

  Optimizations:

  - worker threads that are idle for long time will shutdown.
  - worker threads are woken in LIFO order, which minimizes context switching
  and also ensures that idle timeout works well. LIFO wakeup order ensures
  that active threads stay active, and idle ones stay idle.

  - to minimize spurious wakeups, some items are not put into the queue. Instead
  submit() will pass the data directly to the thread it woke up.
*/
class thread_pool_generic : public thread_pool
{
  /**
   Worker wakeup flags.
  */
  enum worker_wake_reason
  {
    WAKE_REASON_NONE,
    WAKE_REASON_TASK,
    WAKE_REASON_SHUTDOWN
  };

  /* A per-worker  structure.*/
  struct worker_data
  {
    /** Condition variable to wakeup this worker.*/
    std::condition_variable m_cv;

    /** Reason why worker was woken. */
    worker_wake_reason m_wake_reason;

    /** 
      If worker wakes up with WAKE_REASON_TASK, this the task it needs to execute.
    */
    task m_task;

    worker_data() : m_cv(), m_wake_reason(WAKE_REASON_NONE), m_task{ 0, 0 } {}
  };

  /** The task queue */
  circular_queue<task> m_task_queue;

  /* List of standby (idle) workers.*/
  std::vector<worker_data *> m_standby_threads;

  /* Mutex that protects the whole struct, most importantly 
  the standby threads list, and task queue. */
  std::mutex m_mtx;

  /** Timeout after which idle worker shuts down.*/
  std::chrono::milliseconds m_thread_timeout;

  /** How often should timer wakeup.*/
  std::chrono::milliseconds m_timer_interval;

  /** Condition variable, used in pool shutdown-*/
  std::condition_variable m_cv_no_active_threads;
  /** Another condition variable, used in pool shutdown-*/
  std::condition_variable m_cv_no_threads;

  /** Condition variable to signal that task queue is not full*/
  std::condition_variable m_cv_queue_not_full;

  /** Condition variable for the timer thread. Signaled on shutdown.*/
  std::condition_variable m_cv_timer;

  /** The timer thread. Will be join()ed on shutdown.*/
  std::thread m_timer_thread;

  /** Current number of workers (both active and standby)*/
  int m_threads;

  /** Current number of workers that execute tasks. */
  int m_active_threads;

  /** Overall number of dequeued tasks. */
  int m_tasks_dequeued;

  /**Statistic related, number of worker thread wakeups.*/
  int m_wakeups;

  /** 
  Statistic related, number of spurious thread wakeups
  (i.e thread woke up, and the task queue is empty)
  */
  int m_spurious_wakeups;

  /**  The desired concurrency.  This number of workers should be actively executing.*/
  int m_concurrency;

  /** True, if threadpool is being shutdown, false otherwise */
  bool m_in_shutdown;

  /**
    Whether this threadpool uses a timer. With permanent pool (fixed number of
    workers, timer is not started, and m_timer_on will be false.
  */
  bool m_timer_on;

  /** Minimumal number of threads in this pool.*/
  int m_min_threads;

  /** Maximal number of threads in this pool. */
  int m_max_threads;

  void worker_main();
  void worker_end();
  void timer_main();
  bool add_thread();
  bool wake(worker_wake_reason reason, const task *t= nullptr);
  void wake_or_create_thread();
  bool get_task(worker_data *thread_var, task *t);
  bool wait_for_tasks(std::unique_lock<std::mutex> &lk,
                      worker_data *thread_var);
  void timer_start();
  void timer_stop();
public:
  thread_pool_generic(int min_threads, int max_threads);
  ~thread_pool_generic();
  void submit_task(const task &task) override;
  virtual aio *create_native_aio(int max_io) override
  {
#ifdef _WIN32
    return create_win_aio(this, max_io);
#elif defined(__linux__)
    return create_linux_aio(this,max_io);
#else
    return nullptr;
#endif
  }
};

/**
  Register worker in standby list, and wait to be woken.

  @return 
  true  -  thread was woken
  false -  idle wait timeout exceeded (the current thread need to shutdown)
*/
bool thread_pool_generic::wait_for_tasks(std::unique_lock<std::mutex> &lk,
                                   worker_data *thread_data)
{
  assert(m_task_queue.empty());
  assert(!m_in_shutdown);

  thread_data->m_wake_reason= WAKE_REASON_NONE;
  m_standby_threads.push_back(thread_data);
  m_active_threads--;

  for (;;)
  {
    thread_data->m_cv.wait_for(lk, m_thread_timeout);
    if (thread_data->m_wake_reason != WAKE_REASON_NONE)
    {
      return true;
    }

    if (m_threads <= m_min_threads)
    {
      continue;
    }

    /*
      Woke up due to timeout, remove this thread's  from the standby list. In
      all other cases where it is signaled it is removed by the signaling
      thread.
    */
    auto it= std::find(m_standby_threads.begin(), m_standby_threads.end(),
                       thread_data);
    m_standby_threads.erase(it);
    m_active_threads++;
    return false;
  }

  return !m_task_queue.empty() && m_threads >= m_min_threads;
}

/** 
 Workers "get next task" routine.

 A task can be handed over to the current thread directly during submit().
 if thread_var->m_wake_reason == WAKE_REASON_TASK.

 Or a task can be taken from the task queue.
 In case task queue is empty, the worker thread will park (wait for wakeup).
*/
bool thread_pool_generic::get_task(worker_data *thread_var, task *t)
{
  std::unique_lock<std::mutex> lk(m_mtx);
  if (m_task_queue.empty())
  {
    if (m_in_shutdown)
      return false;

    if (!wait_for_tasks(lk, thread_var))
      return false;

    /* Task was handed over directly.*/
    if (thread_var->m_wake_reason == WAKE_REASON_TASK)
    {
      *t= thread_var->m_task;
      thread_var->m_task.m_func= 0;
      return true;
    }

    if (m_task_queue.empty())
      return false;
  }

  /* Dequeue from the task queue.*/
  bool task_queue_was_full= m_task_queue.full();
  *t= m_task_queue.front();
  m_task_queue.pop();
  m_tasks_dequeued++;

  if (task_queue_was_full)
  {
    /*
      There may be threads handing in submit(),
      because the task queue was full. Wake them
    */
    m_cv_queue_not_full.notify_all();
  }
  return true;
}

/** Worker thread shutdown routine. */
void thread_pool_generic::worker_end()
{
  std::lock_guard<std::mutex> lk(m_mtx);
  m_threads--;
  m_active_threads--;

  if (!m_threads && m_in_shutdown)
  {
    /* Signal the destructor that no more threads are left. */
    m_cv_no_threads.notify_all();
  }
}

/* The worker get/execute task loop.*/
void thread_pool_generic::worker_main()
{
  worker_data thread_var;
  task task;

  while (get_task(&thread_var, &task))
  {
    task.m_func(task.m_arg);
  }

  worker_end();
}

void thread_pool_generic::timer_main()
{
  int last_tasks_dequeued= 0;
  int last_threads= 0;
  for (;;)
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    m_cv_timer.wait_for(lk, m_timer_interval);

    if (!m_timer_on || (m_in_shutdown && m_task_queue.empty()))
      return;
    if (m_task_queue.empty())
      continue;

    if (m_active_threads < m_concurrency)
    {
      wake_or_create_thread();
      continue;
    }

    if (!m_task_queue.empty() && last_tasks_dequeued == m_tasks_dequeued &&
        last_threads <= m_threads && m_active_threads == m_threads)
    {
      // no progress made since last iteration. create new
      // thread
      add_thread();
    }
    lk.unlock();
    last_tasks_dequeued= m_tasks_dequeued;
    last_threads= m_threads;
  }
}

/* Create a new worker.*/
bool thread_pool_generic::add_thread()
{
  if (m_threads >= m_max_threads)
    return false;
  m_threads++;
  m_active_threads++;
  std::thread thread(&thread_pool_generic::worker_main, this);
  thread.detach();
  return true;
}

/** Wake a standby thread, and hand the given task over to this thread. */
bool thread_pool_generic::wake(worker_wake_reason reason, const task *t)
{
  assert(reason != WAKE_REASON_NONE);

  if (m_standby_threads.empty())
    return false;
  auto var= m_standby_threads.back();
  m_standby_threads.pop_back();
  m_active_threads++;
  assert(var->m_wake_reason == WAKE_REASON_NONE);
  var->m_wake_reason= reason;
  var->m_cv.notify_one();
  if (t)
  {
    var->m_task= *t;
  }
  m_wakeups++;
  return true;
}

/** Start the timer thread*/
void thread_pool_generic::timer_start()
{
  m_timer_thread = std::thread(&thread_pool_generic::timer_main, this);
  m_timer_on = true;
}


/** Stop the timer thread*/
void thread_pool_generic::timer_stop()
{
  assert(m_in_shutdown || m_max_threads == m_min_threads);
  if(!m_timer_on)
    return;
  m_timer_on = false;
  m_cv_timer.notify_one();
  m_timer_thread.join();
}

thread_pool_generic::thread_pool_generic(int min_threads, int max_threads)
    : m_task_queue(10000),
      m_standby_threads(),
      m_mtx(),
      m_thread_timeout(std::chrono::milliseconds(60000)),
      m_timer_interval(std::chrono::milliseconds(10)),
      m_cv_no_threads(),
      m_cv_timer(),
      m_threads(),
      m_active_threads(),
      m_tasks_dequeued(),
      m_wakeups(),
      m_spurious_wakeups(),
      m_concurrency(std::thread::hardware_concurrency()),
      m_in_shutdown(),
      m_timer_on(),
      m_min_threads(min_threads),
      m_max_threads(max_threads)
{
  if (min_threads != max_threads)
    timer_start();
  if (max_threads < m_concurrency)
    m_concurrency = m_max_threads;
  if (min_threads > m_concurrency)
    m_concurrency = min_threads;
  if (!m_concurrency)
    m_concurrency = 1;
}


void thread_pool_generic::wake_or_create_thread()
{
  assert(!m_task_queue.empty());
  if (!m_standby_threads.empty())
  {
    task &t= m_task_queue.front();
    m_task_queue.pop();
    wake(WAKE_REASON_TASK, &t);
  }
  else
  {
    add_thread();
  }
}


/** Submit a new task*/
void thread_pool_generic::submit_task(const task &task)
{
  std::unique_lock<std::mutex> lk(m_mtx);

  while (m_task_queue.full())
  {
    m_cv_queue_not_full.wait(lk);
  }
  if (m_in_shutdown)
    return;
  m_task_queue.push(task);
  if (m_active_threads < m_concurrency)
    wake_or_create_thread();
}

/**
  Wake  wake up all workers, and wait until they are gone
  Stop the timer.
*/
thread_pool_generic::~thread_pool_generic()
{
  std::unique_lock<std::mutex> lk(m_mtx);
  m_in_shutdown= true;
  /* Wake up idle threads. */
  while (wake(WAKE_REASON_SHUTDOWN))
  {
  }

  while (m_threads)
  {
    m_cv_no_threads.wait(lk);
  }

  lk.unlock();

  timer_stop();

  m_cv_queue_not_full.notify_all();
}

thread_pool *create_thread_pool_generic(int min_threads, int max_threads)
{ 
 return new thread_pool_generic(min_threads, max_threads);
}

} // namespace tpool
