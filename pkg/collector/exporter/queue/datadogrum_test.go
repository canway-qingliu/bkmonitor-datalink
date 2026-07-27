// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package queue

import (
	"testing"
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

func TestDatadogRumBatchUsesLogsBatchSize(t *testing.T) {
	q := NewBatchQueue(Config{
		MetricsBatchSize: 1,
		LogsBatchSize:    2,
		FlushInterval:    time.Minute,
	}, func(string) Config { return Config{} })
	defer q.Close()

	token := define.Token{Original: "rum-token"}
	q.Put(
		&testDatadogRumEvent{CommonEvent: define.NewCommonEvent(token, 1001, mapData("first"))},
		&testDatadogRumEvent{CommonEvent: define.NewCommonEvent(token, 1001, mapData("second"))},
	)

	select {
	case batch := <-q.Pop():
		dataID, err := batch.GetValue("dataid")
		require.NoError(t, err)
		assert.Equal(t, int32(1001), dataID)

		items, err := batch.GetValue("items")
		require.NoError(t, err)
		rumItems, ok := items.([]common.MapStr)
		require.True(t, ok)
		require.Len(t, rumItems, 2)
		assert.Equal(t, "first", rumItems[0]["data"])
		assert.Equal(t, "second", rumItems[1]["data"])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Datadog RUM batch")
	}
}

func TestDatadogRumBatchFlushesOnTick(t *testing.T) {
	q := NewBatchQueue(Config{
		LogsBatchSize: 100,
		FlushInterval: 10 * time.Millisecond,
	}, func(string) Config { return Config{} })
	defer q.Close()

	q.Put(&testDatadogRumEvent{
		CommonEvent: define.NewCommonEvent(define.Token{}, 1002, mapData("tick")),
	})

	select {
	case batch := <-q.Pop():
		items, err := batch.GetValue("items")
		require.NoError(t, err)
		rumItems, ok := items.([]common.MapStr)
		require.True(t, ok)
		require.Len(t, rumItems, 1)
		assert.Equal(t, "tick", rumItems[0]["data"])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Datadog RUM tick batch")
	}
}

func mapData(value string) common.MapStr {
	return common.MapStr{
		"data":   value,
		"source": "192.0.2.1",
	}
}
