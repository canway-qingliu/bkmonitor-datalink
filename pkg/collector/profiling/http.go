// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package profiling

import (
	"net/http"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/tokenparser"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/receiver"
)

func init() {
	receiver.RegisterReadyFunc(define.SourceProfiling, Ready)
}

// Ready 在 Receiver ready() 阶段调用，注册 profiling 拉取接口
func Ready() {
	receiver.RegisterRecvHttpRoute(define.SourceProfiling, []receiver.RouteWithFunc{
		{
			Method:       http.MethodGet,
			RelativePath: "/profiling/config",
			HandlerFunc:  handleProfilingConfig,
		},
	})
}

// handleProfilingConfig 根据请求中的 bk-token 返回对应应用的 profiling 配置
// 无 token 或无 per-token 覆盖时回退到默认配置
func handleProfilingConfig(w http.ResponseWriter, r *http.Request) {
	token := tokenparser.FromHttpRequest(r)
	cfg := GetConfig(token)
	if cfg == nil {
		receiver.WriteErrResponse(w, define.ContentTypeJson, http.StatusNotFound,
			errNoProfilingConfig)
		return
	}

	b, err := json.Marshal(cfg)
	if err != nil {
		receiver.WriteErrResponse(w, define.ContentTypeJson, http.StatusInternalServerError, err)
		return
	}
	receiver.WriteResponse(w, define.ContentTypeJson, http.StatusOK, b)
}