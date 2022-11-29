#include "ob_load_data_direct_task.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/ob_exec_context.h"

#define USING_LOG_PREFIX SQL_ENG

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
        new (buf) ObLoadDataDirectTask(index_, ctx_, stmt_, offset_, end_, stage_process_,
                                       dispatcher_, sstable_writer_);
  }
  return task;
}

int ObLoadDataDirectTask::process() {
  int ret = OB_SUCCESS;
  ObLoadDataDirectDemo *load_direct=nullptr;
  if (OB_ISNULL(load_direct = OB_NEWx(ObLoadDataDirectDemo, (&ctx_.get_allocator())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(load_direct->init(index_, stmt_, offset_, end_, stage_process_,
                                       dispatcher_, sstable_writer_))) {
    LOG_WARN("failed to execute load data stmt", K(ret));
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
