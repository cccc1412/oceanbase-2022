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
  ObLoadDataDirectTask(int64_t index, ObExecContext & ctx,
                       ObLoadDataStmt &stmt, int64_t offset, int64_t end,
                       bool stage_process, ObLoadDispatcher *dispatcher,
                       ObLoadSSTableWriter *sstable_writer=nullptr)
      : index_(index), ctx_(ctx), stmt_(stmt), offset_(offset), end_(end),
        stage_process_(stage_process), dispatcher_(dispatcher),
        sstable_writer_(sstable_writer){};
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override {return sizeof(ObLoadDataDirectTask);};
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  int64_t index_;
  ObExecContext &ctx_;
  ObLoadDataStmt &stmt_;
  int64_t offset_;
  int64_t end_;
  bool stage_process_;
  ObLoadDispatcher *dispatcher_;
  ObLoadSSTableWriter *sstable_writer_;
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataDirectTask);
};

} // namespace sql
} // namespace oceanbase
#endif
