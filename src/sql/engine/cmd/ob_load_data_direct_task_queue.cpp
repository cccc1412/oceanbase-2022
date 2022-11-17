#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_load_data_direct_task_queue.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/net/ob_addr.h"
#include "lib/profile/ob_trace_id.h"

int oceanbase::sql::ObLoadDataDirectTaskQueue::push_task(const share::ObAsyncTask &task) {
  int ret = OB_SUCCESS;
  if(OB_FAIL(ret = this->push(task))) {
    return ret;
  }
  task_cnt_++;
  return ret;
}

void oceanbase::sql::ObLoadDataDirectTaskQueue::run2() {
  int ret = OB_SUCCESS;
  LOG_INFO("async task queue start");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObAddr zero_addr;
    while (!stop_) {
      if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {
        LOG_INFO("[ASYNC TASK QUEUE]", "queue_size", queue_.size());
      }
      share::ObAsyncTask *task = NULL;
      ret = pop(task);
      if (OB_FAIL(ret))  {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("pop task from queue failed", K(ret));
        }
      } else if (NULL == task) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pop return a null task", K(ret));
      } else {
        bool rescheduled = false;
        if (task->get_last_execute_time() > 0) {
          while (!stop_ && OB_SUCC(ret)) {
            int64_t now = ObTimeUtility::current_time();
            int64_t sleep_time = task->get_last_execute_time() + task->get_retry_interval() - now;
            if (sleep_time > 0) {
              ::usleep(static_cast<int32_t>(MIN(sleep_time, SLEEP_INTERVAL)));
            } else {
              break;
            }
          }
        }
        // generate trace id
        ObCurTraceId::init(zero_addr);
        // just do it
        ret = task->process();
        ObThreadCondGuard guard(cnt_cond_);
        submit_cnt_++;
        cnt_cond_.broadcast();
        if (OB_FAIL(ret)) {
          LOG_WARN("task process failed, start retry", "max retry time",
              task->get_retry_times(), "retry interval", task->get_retry_interval(),
              K(ret));
          if (task->get_retry_times() > 0) {
            task->set_retry_times(task->get_retry_times() - 1);
            task->set_last_execute_time(ObTimeUtility::current_time());
            if (OB_FAIL(queue_.push(task))) {
              LOG_ERROR("push task to queue failed", K(ret));
            } else {
              rescheduled = true;
            }
          }
        }
        if (!rescheduled) {
          task->~ObAsyncTask();
          allocator_.free(task);
          task = NULL;
        }
      }
    }
  }
  LOG_INFO("async task queue stop");
}

int oceanbase::sql::ObLoadDataDirectTaskQueue::wait_all_task() {
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cnt_cond_);
  while(submit_cnt_ != task_cnt_) {
    cnt_cond_.wait();
  }
  return ret;
}
