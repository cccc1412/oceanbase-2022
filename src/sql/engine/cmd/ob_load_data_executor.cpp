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

#include "sql/engine/cmd/ob_load_data_executor.h"
#include "share/rc/ob_tenant_base.h"

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

  ObLoadDispatcher dispatcher(PROCESS_THREAD_NUM, LOAD_THREAD_NUM);
  // dispatcher.debug_print();
  ObLoadSSTableWriter sstable_writer;
  if (OB_FAIL(do_execute(ctx, stmt, dispatcher, sstable_writer))) {
    LOG_WARN("do process fail", KR(ret));
  } else {
    LOG_INFO("load data success");
  }
  return ret;
}

int ObLoadDataExecutor::do_execute(ObExecContext &ctx, ObLoadDataStmt &stmt,
                                   ObLoadDispatcher &dispatcher,
                                   ObLoadSSTableWriter &sstable_writer) {
  int ret = OB_SUCCESS;
  ObLoadDataDirectTaskQueue process_async_tq;
  ObLoadDataDirectTaskQueue load_async_tq;
  if (OB_FAIL(do_process(process_async_tq, ctx, stmt, dispatcher,
                         sstable_writer))) {
    LOG_WARN("do process fail", KR(ret));
  } else if (OB_FAIL(do_load(load_async_tq, ctx, stmt, dispatcher,
                             sstable_writer))) {
    LOG_WARN("do load fail", KR(ret));
  } else {
    process_async_tq.wait_task();
    process_async_tq.stop();
    process_async_tq.wait();
    load_async_tq.wait_task();
    load_async_tq.stop();
    load_async_tq.wait();
    if (OB_FAIL(sstable_writer.finish())) {
      LOG_WARN("cannot close sstable", KR(ret));
    }
  }
  return ret;
}

int ObLoadDataExecutor::do_process(ObLoadDataDirectTaskQueue &async_tq,
                                   ObExecContext &ctx, ObLoadDataStmt &stmt,
                                   ObLoadDispatcher &dispatcher,
                                   ObLoadSSTableWriter &sstable_writer) {
  int ret = OB_SUCCESS;
  struct stat st;
  async_tq.set_run_wrapper(MTL_CTX());
  if (stat(stmt.get_load_arguments().file_name_.ptr(), &st) < 0) {
    LOG_WARN("cannot load file");
    ret = OB_FILE_NOT_OPENED;
  } else if (OB_FAIL(
                 async_tq.init(PROCESS_THREAD_NUM, 1 << 10, "ObLoadData1"))) {
    LOG_WARN("cannot init async_tq", KR(ret));
  } else if (OB_FAIL(async_tq.start())) {
    LOG_WARN("cannot start async_tq", KR(ret));
  } else {
    int task_id = 0;
    off64_t size = st.st_size;
    int64_t spilt_size = size / PROCESS_THREAD_NUM;
    for (int i = 0; i < PROCESS_THREAD_NUM; i++) {
      int64_t offset = spilt_size * i;
      int64_t end = spilt_size * (i + 1);
      ObLoadDataDirectTask ObLDDT(i, ctx, stmt, offset, end, true, &dispatcher,
                                  &sstable_writer);
      ObLDDT.set_retry_times(0);
      if (OB_FAIL(async_tq.push_task(ObLDDT))) {
        LOG_WARN("cannot push task", KR(ret));
        break;
      }
    }
  }
  return ret;
}

int ObLoadDataExecutor::do_load(ObLoadDataDirectTaskQueue &async_tq,
                                ObExecContext &ctx, ObLoadDataStmt &stmt,
                                ObLoadDispatcher &dispatcher,
                                ObLoadSSTableWriter &sstable_writer) {
  int ret = OB_SUCCESS;
  async_tq.set_run_wrapper(MTL_CTX());
  if (OB_FAIL(async_tq.init(LOAD_THREAD_NUM, 1 << 10, "ObLoadData2"))) {
    LOG_WARN("cannot init async_tq", KR(ret));
  } else if (OB_FAIL(async_tq.start())) {
    LOG_WARN("cannot start async_tq", KR(ret));
  } else {
    for (int i = 0; i < LOAD_THREAD_NUM; i++) {
      ObLoadDataDirectTask ObLDDT(i, ctx, stmt, 0, 0, false, &dispatcher,
                                  &sstable_writer);
      ObLDDT.set_retry_times(0);
      if (OB_FAIL(async_tq.push_task(ObLDDT))) {
        LOG_WARN("cannot push task", KR(ret));
        break;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase