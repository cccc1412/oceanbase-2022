/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOAD_DATA_EXECUTOR_H_
#define OCEANBASE_LOAD_DATA_EXECUTOR_H_
#include "sql/engine/cmd/ob_load_data_direct_demo.h"
#include "sql/engine/cmd/ob_load_data_direct_task_queue.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObLoadDataStmt;
class ObLoadDataExecutor {

  static const int64_t PROCESS_THREAD_NUM = 4;
  static const int64_t LOAD_THREAD_NUM = 11;

public:
  ObLoadDataExecutor() {}
  virtual ~ObLoadDataExecutor() {}

  int execute(ObExecContext &ctx, ObLoadDataStmt &stmt);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLoadDataExecutor);
  // function members
private:
  int do_execute(ObExecContext &ctx, ObLoadDataStmt &stmt,
                 ObLoadDispatcher &dispatcher,
                 ObLoadSSTableWriter &sstable_writer);
  int do_process(ObLoadDataDirectTaskQueue &async_tq, ObExecContext &ctx,
                 ObLoadDataStmt &stmt, ObLoadDispatcher &dispatcher,
                 ObLoadSSTableWriter &sstable_writer);
  int do_load(ObLoadDataDirectTaskQueue &async_tq, ObExecContext &ctx,
              ObLoadDataStmt &stmt, ObLoadDispatcher &dispatcher,
              ObLoadSSTableWriter &sstable_writer);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_LOAD_DATA_EXECUTOR_H_ */
