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
	"errors"
	"testing"

	"github.com/elastic/beats/libbeat/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
)

type testDatadogRumRecordEvent struct {
	raw []byte
	err error
}

func (e testDatadogRumRecordEvent) DatadogRumEvent() {}

func (e testDatadogRumRecordEvent) MarshalJSON() ([]byte, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.raw, nil
}

func TestDatadogRumConverter(t *testing.T) {
	converter := datadogRumConverter{}
	token := define.Token{LogsDataId: 1234}
	rawEvents := [][]byte{
		[]byte(`{"type":"view","application":{"id":"app"},"unknown":{"keep":true}}`),
		[]byte(`{"type":"error","error":{"message":"boom"}}`),
	}
	record := &define.Record{
		RecordType:    define.RecordDatadogRum,
		RequestClient: define.RequestClient{IP: "192.0.2.10"},
		Token:         token,
		Data: &define.DatadogRumData{
			Events: []define.DatadogRumEvent{
				testDatadogRumRecordEvent{raw: rawEvents[0]},
				testDatadogRumRecordEvent{raw: rawEvents[1]},
			},
		},
	}

	var events []define.Event
	converter.Convert(record, func(got ...define.Event) { events = append(events, got...) })

	require.Len(t, events, 2)
	for i, event := range events {
		assert.Equal(t, define.RecordDatadogRum, event.RecordType())
		assert.Equal(t, int32(1234), event.DataId())
		assert.Equal(t, token, event.Token())
		assert.Equal(t, string(rawEvents[i]), event.Data()["data"])
		assert.Equal(t, "192.0.2.10", event.Data()["source"])
	}
}

func TestDatadogRumConverterInvalidData(t *testing.T) {
	converter := datadogRumConverter{}
	var calls int
	gather := func(...define.Event) { calls++ }

	assert.NotPanics(t, func() {
		converter.Convert(&define.Record{Data: &define.DatadogRumData{}}, gather)
		converter.Convert(&define.Record{Data: define.DatadogRumData{}}, gather)
		converter.Convert(&define.Record{Data: nil}, gather)
		converter.Convert(nil, gather)
	})
	assert.Zero(t, calls)
}

func TestDatadogRumConverterSkipsMarshalErrors(t *testing.T) {
	var events []define.Event
	(datadogRumConverter{}).Convert(&define.Record{
		RequestClient: define.RequestClient{IP: "198.51.100.2"},
		Token:         define.Token{LogsDataId: 1},
		Data: &define.DatadogRumData{Events: []define.DatadogRumEvent{
			testDatadogRumRecordEvent{err: errors.New("marshal failed")},
			testDatadogRumRecordEvent{raw: []byte(`{"type":"action"}`)},
		}},
	}, func(got ...define.Event) { events = append(events, got...) })

	require.Len(t, events, 1)
	assert.Equal(t, `{"type":"action"}`, events[0].Data()["data"])
}

func TestCommonConverterDatadogRum(t *testing.T) {
	conv := NewCommonConverter(&Config{})
	defer conv.Clean()

	var events []define.Event
	conv.Convert(&define.Record{
		RecordType: define.RecordDatadogRum,
		Token:      define.Token{LogsDataId: 42},
		Data: &define.DatadogRumData{Events: []define.DatadogRumEvent{
			testDatadogRumRecordEvent{raw: []byte(`{"type":"view"}`)},
		}},
	}, func(got ...define.Event) { events = append(events, got...) })

	require.Len(t, events, 1)
	assert.Equal(t, int32(42), events[0].DataId())
	assert.IsType(t, common.MapStr{}, events[0].Data())
}
