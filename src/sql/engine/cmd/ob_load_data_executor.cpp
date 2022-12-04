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
  DispatchSortQueue sort_queues[LOAD_THREAD_NUM];
  ObLoadSSTableWriter sstable_writer;
  for (int i = 0; i < LOAD_THREAD_NUM && OB_SUCC(ret); i++) {
    void *buf = nullptr;
    ObArenaAllocator *allocator1 = nullptr;
    ObArenaAllocator *allocator2 = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArenaAllocator)))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_ISNULL(allocator1 = new (buf) ObArenaAllocator(
                             common::ObNewModIds::OB_SQL_LOAD_DATA,
                             common::OB_MALLOC_BIG_BLOCK_SIZE, MTL_ID()))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArenaAllocator)))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_ISNULL(allocator2 = new (buf) ObArenaAllocator(
                             common::ObNewModIds::OB_SQL_LOAD_DATA,
                             common::OB_MALLOC_BIG_BLOCK_SIZE, MTL_ID()))) {
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      sort_queues[i].init(allocator1, allocator2, SORT_QUEUE_BUFFER_SIZE,
                          SORT_QUEUE_SLEEP_TIME, SORT_QUEUE_SLEEP_TIME/2);
    }
  }
  if (OB_SUCC(ret)) {
    // dispatcher.debug_print();
    if (OB_FAIL(
            do_execute(ctx, stmt, dispatcher, sort_queues, sstable_writer))) {
      LOG_WARN("do process fail", KR(ret));
    } else {
      LOG_INFO("load data success");
    }
  }
  return ret;
}

int ObLoadDataExecutor::do_execute(ObExecContext &ctx, ObLoadDataStmt &stmt,
                                   ObLoadDispatcher &dispatcher,
                                   DispatchSortQueue *sort_queues,
                                   ObLoadSSTableWriter &sstable_writer) {
  int ret = OB_SUCCESS;
  ObLoadDataDirectTaskQueue process_async_tq;
  ObLoadDataDirectTaskQueue load_async_tq1;
  ObLoadDataDirectTaskQueue load_async_tq2;
  if (OB_FAIL(do_process(process_async_tq, ctx, stmt, dispatcher))) {
    LOG_WARN("do process fail", KR(ret));
  } else if (OB_FAIL(do_load1(load_async_tq1, ctx, stmt, dispatcher,
                              sort_queues))) {
    LOG_WARN("do load1 fail", KR(ret));
  } else {
    process_async_tq.wait_task();
    process_async_tq.stop();
    process_async_tq.wait();
    // for (int i = 0; i < LOAD_THREAD_NUM; i++) {
    //   sort_queues[i].debug_print(i);
    // }
    if (OB_FAIL(
            do_load2(load_async_tq2, ctx, stmt, sort_queues, sstable_writer))) {
      LOG_WARN("do load2 fail", KR(ret));
    } else {
      load_async_tq1.wait_task();
      load_async_tq1.stop();
      load_async_tq1.wait();
      load_async_tq2.wait_task();
      load_async_tq2.stop();
      load_async_tq2.wait();
      if (OB_FAIL(sstable_writer.finish())) {
        LOG_WARN("cannot close sstable", KR(ret));
      }
    }
  }
  return ret;
}

int ObLoadDataExecutor::do_process(ObLoadDataDirectTaskQueue &async_tq,
                                   ObExecContext &ctx, ObLoadDataStmt &stmt,
                                   ObLoadDispatcher &dispatcher) {
  int ret = OB_SUCCESS;
  struct stat st;
  async_tq.set_run_wrapper(MTL_CTX());
  if (stat(stmt.get_load_arguments().file_name_.ptr(), &st) < 0) {
    LOG_WARN("cannot load file");
    ret = OB_FILE_NOT_OPENED;
  } else if (OB_FAIL(async_tq.init(PROCESS_THREAD_NUM, 1 << 10,
                                   "do_process"))) {
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
      ObLoadDataDirectTask ObLDDT(i, ctx, stmt, offset, end, 0, &dispatcher);
      ObLDDT.set_retry_times(0);
      if (OB_FAIL(async_tq.push_task(ObLDDT))) {
        LOG_WARN("cannot push task", KR(ret));
        break;
      }
    }
  }
  return ret;
}

int ObLoadDataExecutor::do_load1(ObLoadDataDirectTaskQueue &async_tq,
                                 ObExecContext &ctx, ObLoadDataStmt &stmt,
                                 ObLoadDispatcher &dispatcher,
                                 DispatchSortQueue *sort_queues) {
  int ret = OB_SUCCESS;
  async_tq.set_run_wrapper(MTL_CTX());
  if (OB_FAIL(async_tq.init(LOAD_THREAD_NUM, 1 << 10, "do_load1"))) {
    LOG_WARN("cannot init async_tq", KR(ret));
  } else if (OB_FAIL(async_tq.start())) {
    LOG_WARN("cannot start async_tq", KR(ret));
  } else {
    for (int i = 0; i < LOAD_THREAD_NUM; i++) {
      ObLoadDataDirectTask ObLDDT(i, ctx, stmt, 0, 0, 1, &dispatcher,
                                  &sort_queues[i]);
      ObLDDT.set_retry_times(0);
      if (OB_FAIL(async_tq.push_task(ObLDDT))) {
        LOG_WARN("cannot push task", KR(ret));
        break;
      }
    }
  }
  return ret;
}

int ObLoadDataExecutor::do_load2(ObLoadDataDirectTaskQueue &async_tq,
                                 ObExecContext &ctx, ObLoadDataStmt &stmt,
                                 DispatchSortQueue *sort_queues,
                                 ObLoadSSTableWriter &sstable_writer) {
  int ret = OB_SUCCESS;
  async_tq.set_run_wrapper(MTL_CTX());
  if (OB_FAIL(async_tq.init(LOAD_THREAD_NUM - 2, 1 << 10, "do_load2"))) {
    LOG_WARN("cannot init async_tq", KR(ret));
  } else if (OB_FAIL(async_tq.start())) {
    LOG_WARN("cannot start async_tq", KR(ret));
  } else {
    for (int i = 0; i < LOAD_THREAD_NUM; i++) {
      ObLoadDataDirectTask ObLDDT(i, ctx, stmt, 0, 0, 2, nullptr,
                                  &sort_queues[i], &sstable_writer);
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
