#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_load_data_direct_task_queue.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::lib;
using namespace oceanbase::omt;

ObLoadDataDirectTaskQueue::ObLoadDataDirectTaskQueue()
    : is_inited_(false), tsk_cnt_(0), submit_cnt_(0) {};

ObLoadDataDirectTaskQueue::~ObLoadDataDirectTaskQueue() {
  if (is_inited_) {
    tsk_cond_.destroy();
  }
};
int ObLoadDataDirectTaskQueue::init(const int64_t thread_cnt,
                                    const int64_t queue_size,
                                    const char *thread_name) {
  int ret=OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task queue has already been initialized", K(ret));
  } else if(OB_FAIL(tsk_cond_.init(ObWaitEventIds::REENTRANT_THREAD_COND_WAIT))){
    LOG_WARN("fail to init cond, ", KR(ret));
  } else if(OB_FAIL(ObAsyncTaskQueue::init(thread_cnt,queue_size,thread_name))){
    LOG_WARN("fail to init task queue, ", KR(ret));
  } else{
    is_inited_ = true;
  }
  return ret;
}

void ObLoadDataDirectTaskQueue::run2() {
  int ret = OB_SUCCESS;
  ObTenantStatEstGuard stat_est_gurad(MTL_ID());
  ObTenantBase *tenant_base=MTL_CTX();
  Worker::CompatMode mode = ((ObTenant *)tenant_base)->get_compat_mode();
  Worker::set_compatibility_mode(mode);
  LOG_INFO("async task queue start");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObAddr zero_addr;
    while (!stop_) {
      if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {
        //每隔一段时间，打印队列的大小
        LOG_INFO("[ASYNC TASK QUEUE]", "queue_size", queue_.size());
      }
      ObAsyncTask *task = NULL;
      ret = pop(task);
      if (OB_FAIL(ret)) {
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
            int64_t sleep_time = task->get_last_execute_time() +
                                 task->get_retry_interval() - now;
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
        if (OB_FAIL(ret)) {
          LOG_WARN("task process failed, start retry", "max retry time",
                   task->get_retry_times(), "retry interval",
                   task->get_retry_interval(), K(ret));
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
          ObThreadCondGuard guard(tsk_cond_);
          submit_cnt_++;
          tsk_cond_.broadcast();
          task->~ObAsyncTask();
          allocator_.free(task);
          task = NULL;
        }
      }
    }
  }
  LOG_INFO("async task queue stop");
}

int ObLoadDataDirectTaskQueue::push_task(const ObAsyncTask &task) {
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret = push(task))) {
    tsk_cnt_++;
  }
  return ret;
}

void ObLoadDataDirectTaskQueue::wait_task() {
  ObThreadCondGuard guard(tsk_cond_);
  while (submit_cnt_ < tsk_cnt_) {
    tsk_cond_.wait();
  }
}