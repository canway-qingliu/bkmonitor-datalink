// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package converter

import (
	"github.com/elastic/beats/libbeat/common"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/json"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

type datadogRumEvent struct {
	define.CommonEvent
}

func (e datadogRumEvent) RecordType() define.RecordType {
	return define.RecordDatadogRum
}

type datadogRumConverter struct{}

func (c datadogRumConverter) Clean() {}

func (c datadogRumConverter) ToEvent(token define.Token, dataID int32, data common.MapStr) define.Event {
	return datadogRumEvent{define.NewCommonEvent(token, dataID, data)}
}

func (c datadogRumConverter) ToDataID(record *define.Record) int32 {
	if record == nil {
		return -1
	}
	return record.Token.LogsDataId
}

func (c datadogRumConverter) Convert(record *define.Record, f define.GatherFunc) {
	if record == nil {
		logger.Warnf("skip Datadog RUM conversion for nil record")
		return
	}

	rumData, ok := record.Data.(*define.DatadogRumData)
	if !ok || rumData == nil {
		logger.Warnf("skip Datadog RUM conversion for invalid data type %T", record.Data)
		return
	}
	if len(rumData.Events) == 0 || f == nil {
		return
	}

	dataID := c.ToDataID(record)
	events := make([]define.Event, 0, len(rumData.Events))
	for _, rumEvent := range rumData.Events {
		if rumEvent == nil {
			logger.Warnf("skip nil Datadog RUM event")
			continue
		}

		raw, err := json.Marshal(rumEvent)
		if err != nil {
			logger.Warnf("skip Datadog RUM event that cannot be marshaled: %v", err)
			continue
		}

		events = append(events, c.ToEvent(record.Token, dataID, common.MapStr{
			"data":   string(raw),
			"source": record.RequestClient.IP,
		}))
	}
	if len(events) > 0 {
		f(events...)
	}
}
