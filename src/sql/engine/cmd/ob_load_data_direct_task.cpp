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
    task = new (buf) ObLoadDataDirectTask(ctx_, stmt_, offset_,end_,load_direct_);
  }
  return task;
}

int ObLoadDataDirectTask::process() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare())) {
    LOG_WARN("prepare error", K(ret));
  } else{
    if(OB_FAIL(load_direct_->execute(ctx_, stmt_))){
      LOG_WARN("failed to execute load data stmt", K(ret));
    }
    load_direct_->~ObLoadDataDirectDemo();
    load_direct_ = nullptr;
  }
  return ret;
}

int ObLoadDataDirectTask::prepare() {
  int ret = OB_SUCCESS;
  if (load_direct_)
    return ret;
  if (OB_ISNULL(load_direct_ =
                    OB_NEWx(ObLoadDataDirectDemo, (&ctx_.get_allocator())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(load_direct_->init(stmt_, offset_, end_))) {
    LOG_WARN("failed to execute load data stmt", K(ret));
    load_direct_->~ObLoadDataDirectDemo();
    load_direct_=nullptr;
  }
  return ret;
}