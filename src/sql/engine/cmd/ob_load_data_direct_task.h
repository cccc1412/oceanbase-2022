#ifndef OB_LOAD_DATA_DIRECT_TASK
#define OB_LOAD_DATA_DIRECT_TASK

#include "lib/charset/ob_mysql_global.h"
#include "lib/thread/ob_async_task_queue.h"
#include "ob_load_data_direct_demo.h"
#include "ob_load_data_impl.h"

namespace oceanbase {
namespace sql {

class ObLoadDataDirectTask : public share::ObAsyncTask {
public:
  ObLoadDataDirectTask(ObExecContext &ctx, ObLoadDataStmt &stmt, int64_t offset, int64_t end, share::ObTenantBase *obt,
                       ObLoadDataDirectDemo *load_direct=nullptr)
      : ctx_(ctx), stmt_(stmt), offset_(offset),
        end_(end), obt_(obt), load_direct_(load_direct){};
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override {return sizeof(*this);};
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  int prepare();
  ObExecContext &ctx_;
  ObLoadDataStmt &stmt_;
  int64_t offset_;
  int64_t end_;
  share::ObTenantBase *obt_;
  ObLoadDataDirectDemo *load_direct_ = nullptr;
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataDirectTask);
};

} // namespace sql
} // namespace oceanbase
#endif
