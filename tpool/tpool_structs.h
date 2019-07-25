/* Copyright(C) 2019 MariaDB Corporation

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

#pragma once
#include <vector>
#include <stack>
#include <mutex>
#include <condition_variable>
#include <assert.h>

namespace tpool
{

enum cache_notification_mode
{
  NOTIFY_ONE,
  NOTIFY_ALL
};

/**
  Generic "pointer" cache of a fixed size
  with fast put/get operations.

  Compared to STL containers, is faster/does not
  do allocations. However, put() operation will wait
  if there is no free items.
*/
template<typename T> class cache
{
  std::mutex m_mtx;
  std::condition_variable m_cv;
  std::vector<T>  m_base;
  std::vector<T*> m_cache;
  cache_notification_mode m_notification_mode;
public:
  cache(size_t count, cache_notification_mode mode= NOTIFY_ALL):
  m_mtx(), m_cv(), m_base(count),m_cache(count), m_notification_mode(mode)
  {
    for(size_t i = 0 ; i < count; i++)
      m_cache[i]=&m_base[i];
  }

  T* get()
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    while(m_cache.empty())
     m_cv.wait(lk);
    T* ret = m_cache.back();
    m_cache.pop_back();
    return ret;
  }
  
  void put(T *ele)
  {
    std::unique_lock<std::mutex> lk(m_mtx);
    m_cache.push_back(ele);
    if (m_notification_mode == NOTIFY_ONE)
      m_cv.notify_one();
    else if(m_cache.size() == 1)
      m_cv.notify_all();
  }
};


/**
  Circular, fixed size queue
  used for the task queue.

  Compared to STL queue, this one is
  faster, and does not do memory allocations
*/
template <typename T> class circular_queue
{
public:
  circular_queue(size_t N)
    : m_capacity(N + 1), m_buffer(m_capacity), m_head(), m_tail()
  {
  }
  bool empty() { return m_head == m_tail; }
  bool full() { return (m_head + 1) % m_capacity == m_tail; }
  void push(T ele)
  {
    assert(!full());
    m_buffer[m_head] = ele;
    m_head = (m_head + 1) % m_capacity;
  }
  T& front()
  {
    assert(!empty());
    return m_buffer[m_tail];
  }
  void pop()
  {
    assert(!empty());
    m_tail = (m_tail + 1) % m_capacity;
  }
  size_t size()
  {
    if (m_head < m_tail)
      return m_tail - m_head;
    return m_capacity - m_head - 1 + m_tail;
  }

private:
  size_t m_capacity;
  std::vector<T> m_buffer;
  size_t m_head;
  size_t m_tail;
};

}