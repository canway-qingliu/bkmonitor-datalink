// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/*
# TokenChecker: Token 校验器

processor:
  # 固定 token
  - name: "token_checker/fixed"
    config:
      type: "fixed"
      fixed_token: "token1"
      resource_key: "bk.data.token"
      bk_tenant_id: tenant-a
      bk_biz_id: 2
      bk_app_name: app-a
      app_id: 123
      traces_dataid: 1000
      metrics_dataid: 1001
      logs_dataid: 1002

  # proxy token 校验规则
  - name: "token_checker/proxy"
    config:
      token: "xxxxxxx"
      dataid: 1001

  # ase256 校验规则
  - name: "token_checker/aes256"
    config:
      type: "aes256"
      resource_key: "bk.data.token"
      salt: "bk" # 加盐解密标识
      decoded_iv: "bkbkbkbkbkbkbkbk"
      decoded_key: "81be7fc6-5476-4934-9417-6d4d593728db"

  # aes256+子配置 dataid 校验规则
  - name: "token_checker/aes256WithMeta"
    config:
      type: "aes256WithMeta"
      resource_key: "bk.data.token"
      salt: "bk" # 加盐解密标识
      decoded_iv: "bkbkbkbkbkbkbkbk"
      decoded_key: "81be7fc6-5476-4934-9417-6d4d593728db"

  # aes256+子配置 dataid 校验规则
  - name: "token_checker/combine"
    config:
      type: "aes256WithMeta|fixed"
      # aes256 配置
      resource_key: "bk.data.token"
      salt: "bk" # 加盐解密标识
      decoded_iv: "bkbkbkbkbkbkbkbk"
      decoded_key: "81be7fc6-5476-4934-9417-6d4d593728db"
      # fixed 配置
      fixed_token: foobar
      traces_dataid: 1000
      metrics_dataid: 1001
      logs_dataid: 1002
      profiles_dataid: 1002

Trace 原 Flat item 的 bk_tenant_id、bk_biz_id 和 app_id 来自已校验的 Token，
不采用客户端 Span/Resource 属性。AES v0/v1 Token 不编码租户和 app_id；
aes256WithMeta 从同一 Token 的受信应用配置补齐它们。
旧独占应用的子配置若已有受信租户和 app_id，也须渲染这两个字段；
traces_shared 只表示共享 DataID 路由，不能作为是否输出身份的条件。
receiver 在校验成功后给 Trace Record 固定首次接收时间和请求 ID；
converter 按请求内 Span 顺序生成稳定 source_event_id，原队列重试不改写。

租户共享 Trace DataID 的受信 Token 子配置须显式设置 traces_shared: true，
并携带正数 app_id、bk_biz_id、traces_dataid，以及非空 bk_tenant_id、bk_app_name。
共享请求内任何 Resource Token 无法解码或成员身份不完整时整体拒绝；
converter 仅在共享 Trace 模式下用受信业务 ID 派生顶层 space_uid，覆盖客户端同名值；
旧独占 Trace Token 不设置此开关，继续沿用原有缺失 Resource 的处理方式。

租户共享 Metric DataID 的受信 Token 子配置须显式设置 metrics_shared: true，
并携带正数 app_id、bk_biz_id、metrics_dataid，以及非空 bk_tenant_id、bk_app_name。
缺少身份、混合不同共享成员 Token，或共享 Metric 请求含无法解码的 Resource Token 时整体拒绝。
旧独占 Token 不设置此开关，派生指标维度保持原样。
*/

package tokenchecker
