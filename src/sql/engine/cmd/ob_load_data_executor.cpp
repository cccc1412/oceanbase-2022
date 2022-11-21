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
#include "sql/engine/cmd/ob_load_data_direct_task_queue.h"

#include "sql/engine/cmd/ob_load_data_executor.h"

#include "lib/oblog/ob_log_module.h"
#include "ob_load_data_direct_task.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/ob_exec_context.h"

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

  ObLoadExternalSort external_sort;
  ObLoadSSTableWriter sstable_writer;
  if (OB_FAIL(do_process(ctx, stmt, external_sort, sstable_writer))) {
    LOG_WARN("do process fail", KR(ret));
  } else if (OB_FAIL(do_load(ctx, stmt, external_sort, sstable_writer))) {
    LOG_WARN("do load fail", KR(ret));
  } else {
    LOG_INFO("load data success");
  }
  return ret;
}

int ObLoadDataExecutor::do_process(ObExecContext &ctx, ObLoadDataStmt &stmt,
                                   ObLoadExternalSort &external_sort,
                                   ObLoadSSTableWriter &sstable_writer) {
  int ret = OB_SUCCESS;
  struct stat st;
  ObLoadDataDirectTaskQueue async_tq;
  async_tq.set_run_wrapper(MTL_CTX());
  if (stat(stmt.get_load_arguments().file_name_.ptr(), &st) < 0) {
    LOG_WARN("cannot load file");
    ret = OB_FILE_NOT_OPENED;
  } else if (OB_FAIL(
                 async_tq.init(PROCESS_THREAD_NUM, 1 << 10, "ObLoadDataExe"))) {
    LOG_WARN("cannot init async_tq", KR(ret));
  } else if (OB_FAIL(async_tq.start())) {
    LOG_WARN("cannot start async_tq", KR(ret));
  } else {
    off64_t size = st.st_size;
    int64_t offset = 0;
    while (offset < size) {
      ObLoadDataDirectTask ObLDDT(ctx, stmt, offset, offset + FILE_SPILT_SIZE,
                                  false, &external_sort, &sstable_writer);
      if (OB_FAIL(async_tq.push_task(ObLDDT))) {
        LOG_WARN("cannot push task", KR(ret));
        goto out;
      };
      offset += FILE_SPILT_SIZE;
    }
    async_tq.wait_task();
  }
out:
  async_tq.stop();
  async_tq.wait();
  return ret;
}

int ObLoadDataExecutor::do_load(ObExecContext &ctx, ObLoadDataStmt &stmt,
                 ObLoadExternalSort &external_sort,
                 ObLoadSSTableWriter &sstable_writer) {
  int ret = OB_SUCCESS;
  ObLoadDataDirectTaskQueue async_tq;
  async_tq.set_run_wrapper(MTL_CTX());
  if (OB_FAIL(external_sort.close())) {
      LOG_WARN("cannot close sort", KR(ret));
  } else if (OB_FAIL(async_tq.init(IO_THREAD_NUM, 1 << 10, "ObLoadDataExe"))) {
    LOG_WARN("cannot init async_tq", KR(ret));
  } else if (OB_FAIL(async_tq.start())) {
    LOG_WARN("cannot start async_tq", KR(ret));
  } else {
    for (int i = 0; i < IO_THREAD_NUM; i++) {
      ObLoadDataDirectTask ObLDDT(ctx, stmt, 0, 0, true, &external_sort,
                                  &sstable_writer);
      if (OB_FAIL(async_tq.push_task(ObLDDT))) {
        LOG_WARN("cannot push task", KR(ret));
        goto out;
      };
    }
    async_tq.wait_task();
  }
  if (OB_FAIL(sstable_writer.close())) {
    LOG_WARN("cannot close sstable", KR(ret));
  }
out:
  async_tq.stop();
  async_tq.wait();
  return ret;
}

} // namespace sql
} // namespace oceanbase
