// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/*
# TokenReplacer: Token 替换器

processor:
  # token 映射
  - name: "token_replacer/token_mapping"
    config:
      type: "token_mapping"
      resource_key: "bk.data.token"
      replace_list:
        - original: "Ymtia2JrYmtia2JrYmtiax2iPbN8PyIpPO1zEQjSCDjXByL7Edu+7gVoUSfpQaUoS9GRBPtIJlol6HwUBR5YrQ=="
          replace: "Ymtia2JrYmtia2JrYmtia5PPuc5jRabnfiysyC6d+vCpABwUjGqw0N4W5EmC6+W/XzzmLGNg+CjvZBvtg1dBPQ=="

  # 应用名称替换
  - name: "token_replacer/app_name"
    config:
      type: "app_name"
      resource_key: "bk.data.token"
*/

package tokenreplacer
