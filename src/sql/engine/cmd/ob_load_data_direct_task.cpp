#include "ob_load_data_direct_task.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/ob_exec_context.h"
#include <sys/sysinfo.h>
#define USING_LOG_PREFIX SQL_ENG
#include <sched.h>
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::common;

ObAsyncTask *ObLoadDataDirectTask::deep_copy(char *buf,
                                             const int64_t buf_size) const{
  ObLoadDataDirectTask *task = nullptr;
  if (NULL == buf || buf_size < (sizeof(*task))) {
    LOG_WARN("invalid argument", KP(buf), K(buf_size));
  } else {
    task =
        new (buf) ObLoadDataDirectTask(ctx_, stmt_, offset_, end_, processed_,
                                       external_sort_, sstable_writer_,index_);
  }
  return task;
}

int ObLoadDataDirectTask::process() {
  int ret = OB_SUCCESS;
  //cpu_set_t mask;  //CPU核的集合
  //cpu_set_t get;   //获取在集合中的CPU
  //CPU_ZERO(&mask);    //置空
  //CPU_SET(index_ % get_nprocs(),&mask);   //设置亲和力值
  //if(sched_setaffinity(0, sizeof(mask), &mask)) {
  //  LOG_WARN("warning ! set affinity failed!");      
  //}

  ObLoadDataDirectDemo *load_direct=nullptr;
  if (OB_ISNULL(load_direct = OB_NEWx(ObLoadDataDirectDemo, (&ctx_.get_allocator())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(load_direct->init(stmt_, offset_, end_, processed_,
                                       external_sort_, sstable_writer_))) {
    LOG_WARN("failed to execute load data stmt", K(ret));
  } else if (!processed_ && OB_FAIL(external_sort_->init_external_sort())) {
    LOG_WARN("failed to init macro external sort", K(ret));
  } else if (processed_ && OB_FAIL(sstable_writer_->init_macro_block_writer(index_))) {
    LOG_WARN("failed to init macro block writer", K(ret));
  } else {
    if (OB_FAIL(load_direct->execute(ctx_, stmt_))) {
      LOG_WARN("failed to execute load data stmt", K(ret));
    }
  }
  if (load_direct) {
    load_direct->~ObLoadDataDirectDemo();
    ctx_.get_allocator().free(load_direct);
  }
  return ret;
}
