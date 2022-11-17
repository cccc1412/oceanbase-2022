#ifndef OB_LOAD_DATA_DIRECT_TASK_QUEUE
#define OB_LOAD_DATA_DIRECT_TASK_QUEUE

#include "lib/charset/ob_mysql_global.h"
#include "lib/thread/ob_async_task_queue.h"
//#include "ob_load_data_direct_demo.h"
//#include "ob_load_data_impl.h"

namespace oceanbase {
namespace sql {

class ObLoadDataDirectTaskQueue : public share::ObAsyncTaskQueue {
public:
  ObLoadDataDirectTaskQueue(): submit_cnt_(0), task_cnt_(0) {}
  virtual ~ObLoadDataDirectTaskQueue() {}
  int push_task(const share::ObAsyncTask &task);
  int wait_all_task();
  virtual void run2() override;
private:
  common::ObThreadCond cnt_cond_;
  int submit_cnt_;
  int task_cnt_;
};

} // namespace sql
} // namespace oceanbase  
#endif
