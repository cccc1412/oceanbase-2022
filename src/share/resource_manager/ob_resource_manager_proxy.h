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

#ifndef _OB_SHARE_RESOURCE_MANAGER_RES_MGR_PROXY_H_
#define _OB_SHARE_RESOURCE_MANAGER_RES_MGR_PROXY_H_

#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/resource_manager/ob_resource_plan_info.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObString;
class ObObj;
}
namespace share
{
class ObResourceManagerProxy
{
public:
  ObResourceManagerProxy();
  virtual ~ObResourceManagerProxy();
  int create_plan(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObObj &comment);
  // This procedure deletes the specified plan as well as
  // all the plan directives to which it refers.
  int delete_plan(
      uint64_t tenant_id,
      const common::ObString &plan);
  int create_consumer_group(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &consumer_group,
      const common::ObObj &comments,
      int64_t consumer_group_id = -1);
  int create_consumer_group(
      uint64_t tenant_id,
      const common::ObString &consumer_group,
      const common::ObObj &comment);
  int delete_consumer_group(
      uint64_t tenant_id,
      const common::ObString &consumer_group);
  int create_plan_directive(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      const common::ObObj &comment,
      const common::ObObj &mgmt_p1,
      const common::ObObj &utilization_limit);
  int create_plan_directive(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      const common::ObObj &comment,
      const common::ObObj &mgmt_p1,
      const common::ObObj &utilization_limit);
  // 这里之所以直接传入 ObObj，而不是传入 ObString 或 int
  // 是为了便于判断传入的参数是否是缺省，如果缺省则 ObObj.is_null() 是 true
  int update_plan_directive(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      const common::ObObj &comment,
      const common::ObObj &mgmt_p1,
      const common::ObObj &utilization_limit);
  int delete_plan_directive(
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group);
  int get_all_plan_directives(
      uint64_t tenant_id,
      const common::ObString &plan,
      common::ObIArray<ObPlanDirective> &directives);

  // process mapping rules
  int replace_mapping_rule(
    uint64_t tenant_id,
    const common::ObString &attribute,
    const common::ObString &value,
    const common::ObString &consumer_group);
  int get_all_resource_mapping_rules(
      uint64_t tenant_id,
      common::ObIArray<ObResourceMappingRule> &rules);
  int get_all_resource_mapping_rules_by_user(
      uint64_t tenant_id,
      const common::ObString &plan,
      common::ObIArray<ObResourceUserMappingRule> &rules);
  int check_if_plan_exist(
      uint64_t tenant_id,
      const common::ObString &plan,
      bool &exist);
private:
  int allocate_consumer_group_id(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      int64_t &group_id);
  int check_if_plan_directive_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &plan,
      const common::ObString &group,
      bool &exist);
  int check_if_plan_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &plan,
      bool &exist);
  int check_if_consumer_group_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &group,
      bool &exist);
  int check_if_user_exist(
      common::ObMySQLTransaction &trans,
      uint64_t tenant_id,
      const common::ObString &user_name,
      bool &exist);
  // helper func, 便于集中获取百分比的值，数值范围为 [0, 100]
  int get_percentage(const char *name, const common::ObObj &obj, int64_t &v);
public:
  class TransGuard {
  public:
    TransGuard(common::ObMySQLTransaction &trans,
               const uint64_t tenant_id,
               int &ret);
    ~TransGuard();
    // 判断 trans 是否成功初始化
    bool ready();
  private:
    common::ObMySQLTransaction &trans_;
    int &ret_;
  };
private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObResourceManagerProxy);
};
}
}
#endif /* _OB_SHARE_RESOURCE_MANAGER_RES_MGR_PROXY_H_ */
//// end of header file

