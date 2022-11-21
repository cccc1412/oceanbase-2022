#ifndef OB_LOAD_DATA_DIRECT_TASK_QUEUE
#define OB_LOAD_DATA_DIRECT_TASK_QUEUE

#include "lib/thread/ob_async_task_queue.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/worker.h"
#include "observer/omt/ob_tenant.h"


namespace oceanbase {
namespace sql {

class ObLoadDataDirectTaskQueue : public share::ObAsyncTaskQueue {
public:
  ObLoadDataDirectTaskQueue();
  virtual ~ObLoadDataDirectTaskQueue();
  int init(const int64_t thread_cnt, const int64_t queue_size,
           const char *thread_name = nullptr);
  int push_task(const ObAsyncTask &task);
  void wait_task();
protected:
  virtual void run2() final;
  bool is_inited_;
private:
  common::ObThreadCond tsk_cond_;
  int tsk_cnt_;
  int submit_cnt_;
};

} // namespace sql
} // namespace oceanbase  
#endif
