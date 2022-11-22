/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/thread/ob_async_task_queue.h"
#include "share/rc/ob_tenant_base.h"

#include "sql/engine/cmd/ob_load_data_executor.h"

#include "lib/oblog/ob_log_module.h"
#include "ob_load_data_direct_task.h"
#include "sql/engine/cmd/ob_load_data_direct_demo.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_load_data_direct_task_queue.h"
#include <stdlib.h>

namespace oceanbase {
namespace sql {
int ObLoadDataExecutor::execute(ObExecContext &ctx, ObLoadDataStmt &stmt) {
  int ret = OB_SUCCESS;
  if (!stmt.get_load_arguments().is_csv_format_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid resolver results", K(ret));
    return ret;
  }

  ObLoadSSTableWriter sstable_writer;
  ObLoadDataDirectTaskQueue async_tq;
  ObLoadDataDirectTaskQueue async_tq2;
  async_tq.set_run_wrapper(MTL_CTX());
  async_tq2.set_run_wrapper(MTL_CTX());
  async_tq.init(1, 1 << 10, "ObLoadDataExe");
  async_tq2.init(1, 1<<10,"ObLoadDataExe2");

  struct stat st;
  if (stat(stmt.get_load_arguments().file_name_.ptr(), &st) < 0) {
   return OB_FILE_NOT_OPENED;
  } else {
    ObLoadDataDirectDemo *virtual_load_direct_ = OB_NEWx(ObLoadDataDirectDemo, (&ctx.get_allocator()));
    virtual_load_direct_->external_sort_.is_inited_ = true;
    virtual_load_direct_->external_sort_.external_sort_.is_inited_ = true;
    virtual_load_direct_->external_sort_.external_sort_.memory_sort_round_.is_inited_ = true;
    off64_t size = st.st_size;
    int64_t offset = 0;
    while (offset < size) {
      ObLoadDataDirectTask ObLDDT(ctx, stmt, offset, offset + FILE_SPILT_SIZE,
                                  &sstable_writer, false);
      if(OB_FAIL(ret = async_tq.push_task(ObLDDT))){
        LOG_WARN("cannot push task");
        return ret;
      };
      offset += FILE_SPILT_SIZE;
    }
    async_tq.start();
    async_tq.wait_all_task();
    async_tq.stop();
    if (OB_FAIL(virtual_load_direct_->external_sort_.close())) {
      LOG_WARN("cannot close sort", KR(ret));
    } else {
      for (int i = 0; i < 1; i++) {
        ObLoadDataDirectTask ObLDDT(ctx, stmt, 0, 0,
                                    &sstable_writer, true);
        if (OB_FAIL(ret = async_tq2.push_task(ObLDDT))) {
          LOG_WARN("cannot push task");
          return ret;
        };
      }
      async_tq2.start();
      async_tq2.wait_all_task();
      async_tq2.stop();
    }
    if (OB_FAIL(sstable_writer.close())) {
      LOG_WARN("cannot close sstable", KR(ret));
    }
    virtual_load_direct_->external_sort_.external_sort_.clean_up();
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
