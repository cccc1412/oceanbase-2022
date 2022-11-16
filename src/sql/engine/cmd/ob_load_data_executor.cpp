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

#include "share/rc/ob_tenant_base.h"
#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/cmd/ob_load_data_executor.h"

#include "lib/oblog/ob_log_module.h"
#include "sql/engine/cmd/ob_load_data_direct_demo.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/ob_exec_context.h"
#include <stdlib.h>
#include "ob_load_data_direct_task.h"

namespace oceanbase
{
namespace sql
{
int ObLoadDataExecutor::execute(ObExecContext &ctx, ObLoadDataStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (!stmt.get_load_arguments().is_csv_format_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid resolver results", K(ret));
    return ret;
  }

  share::ObAsyncTaskQueue async_tq;
  async_tq.init(1, 1 << 10, "ObLoadDataExecutor");

  struct stat st;
  if (stat(stmt.get_load_arguments().file_name_.ptr(), &st) < 0) {
    return OB_FILE_NOT_OPENED;
  } else {
    off64_t size = st.st_size;
    int64_t offset = 0;
    while (offset < size) {
      share::ObTenantBase *obt = MTL_CTX();
      ObLoadDataDirectTask ObLDDT(ctx,stmt,offset,offset+FILE_SPILT_SIZE, obt);
      async_tq.push(ObLDDT);
      offset += FILE_SPILT_SIZE;
    }
    if (OB_FAIL(async_tq.start())) {
      LOG_WARN("cannot start async_tq", K(ret));
    } else {
      async_tq.wait();
    }
  }
  return ret;
}

} // sql
} // oceanbase
