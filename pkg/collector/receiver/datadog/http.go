// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package datadog

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/define"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/tokenparser"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/internal/utils"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/pipeline"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/collector/receiver"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/logger"
)

const (
	routeRumV1 = "/api/v2/rum"
	routeRumV2 = "/api/v2/rum/events"
)

func init() {
	receiver.RegisterReadyFunc(define.SourceDatadog, Ready)
}

var (
	metricMonitor = receiver.DefaultMetricMonitor.Source(define.SourceDatadog)
	otelConverter = NewOtelConverter()
)

func Ready() {
	receiver.RegisterRecvHttpRoute(define.SourceDatadog, []receiver.RouteWithFunc{
		{
			Method:       http.MethodPost,
			RelativePath: routeRumV1,
			HandlerFunc:  httpSvc.RumV1,
		},
		{
			Method:       http.MethodPost,
			RelativePath: routeRumV2,
			HandlerFunc:  httpSvc.RumV2,
		},
	})
}

type HttpService struct {
	receiver.Publisher
	pipeline.Validator
}

type convertedRecord struct {
	rtype define.RecordType
	data  interface{}
}

var httpSvc HttpService

// convertDataToDatadogEventV2 将原始数据转换为 RUMEventV2 格式
func convertDataToDatadogEventV2(data interface{}) (RUMEventV2, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return RUMEventV2{}, errors.New("data must be a map")
	}

	event := RUMEventV2{}

	// 提取基本字段
	if v, ok := m["type"].(string); ok {
		event.Type = v
	}
	if v, ok := m["event_type"].(string); ok {
		event.EventType = v
	}
	if v, ok := m["date"].(float64); ok {
		event.Date = int64(v)
	}

	// 提取顶层属性
	if v, ok := m["source"].(string); ok {
		event.Source = v
	}
	if v, ok := m["service"].(string); ok {
		event.Service = v
	}
	if v, ok := m["version"].(string); ok {
		event.Version = v
	}
	if v, ok := m["ddtags"].(string); ok {
		event.DDTags = v
	}

	// 提取嵌套对象（保持完整的 map 结构）
	event.DD = getMapValue(m, "_dd")
	event.Application = getMapValue(m, "application")
	event.View = getMapValue(m, "view")
	event.Session = getMapValue(m, "session")
	event.Connectivity = getMapValue(m, "connectivity")
	event.User = getMapValue(m, "usr")
	event.Display = getMapValue(m, "display")

	// 根据事件类型提取相应的详细信息对象
	switch event.Type {
	case "resource":
		event.Resource = getMapValue(m, "resource")
	case "error":
		event.Error = getMapValue(m, "error")
	case "action":
		event.Action = getMapValue(m, "action")
	case "long_task":
		event.LongTask = getMapValue(m, "long_task")
	case "view":
		// view 信息已经在顶层的 View 字段
	default:
		// 其他未知类型，尝试从 data 字段提取
		event.Data = getMapValue(m, "data")
	}

	// 提取 meta 和 context（如果存在）
	event.Meta = getMapValue(m, "meta")
	event.Context = getMapValue(m, "context")

	return event, nil
}

// getMapValue 从map中获取map类型的值
func getMapValue(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key].(map[string]interface{}); ok {
		return v
	}
	return nil
}

// transformRecord 将原始数据转换为 OTEL 格式。
func transformRecord(data interface{}) (ConversionResult, error) {
	// 先转换为 RUMEventV2
	event, err := convertDataToDatadogEventV2(data)
	if err != nil {
		return ConversionResult{}, err
	}

	// 日志输出转换过程
	logger.Warnf("convertedRUMEventV2: type=%s, event_type=%s, has_data=%v", event.Type, event.EventType, event.Data != nil)

	// 使用转换器进行转换
	conversionResult := otelConverter.ToOTEL(&event)

	return conversionResult, nil
}

// splitConversionResult 按信号类型拆分转换结果。
func splitConversionResult(result ConversionResult) []convertedRecord {
	records := make([]convertedRecord, 0, 3)

	if result.Logs.LogRecordCount() > 0 {
		records = append(records, convertedRecord{rtype: define.RecordLogs, data: result.Logs})
	}
	if result.Traces.SpanCount() > 0 {
		records = append(records, convertedRecord{rtype: define.RecordTraces, data: result.Traces})
	}
	if result.Metrics.MetricCount() > 0 {
		records = append(records, convertedRecord{rtype: define.RecordMetrics, data: result.Metrics})
	}

	return records
}

// debugLogConversionResult 使用 pdata 官方 JSON marshaler 输出调试日志。
func debugLogConversionResult(conversionResult ConversionResult) {
	debugLogPDataLogs(conversionResult.Logs)
	debugLogPDataTraces(conversionResult.Traces)
	debugLogPDataMetrics(conversionResult.Metrics)
}

// debugLogPDataLogs 输出 logs 调试内容。
func debugLogPDataLogs(logs plog.Logs) {
	if logs.LogRecordCount() == 0 {
		return
	}

	jsonBytes, err := plog.NewJSONMarshaler().MarshalLogs(logs)
	if err != nil {
		logger.Debugf("marshal pdata logs failed: %v", err)
		return
	}

	logger.Debugf("conversionResult.logs=%s", string(jsonBytes))
}

// debugLogPDataTraces 输出 traces 调试内容。
func debugLogPDataTraces(traces ptrace.Traces) {
	if traces.SpanCount() == 0 {
		return
	}

	jsonBytes, err := ptrace.NewJSONMarshaler().MarshalTraces(traces)
	if err != nil {
		logger.Debugf("marshal pdata traces failed: %v", err)
		return
	}

	logger.Debugf("conversionResult.traces=%s", string(jsonBytes))
}

// debugLogPDataMetrics 输出 metrics 调试内容。
func debugLogPDataMetrics(metrics pmetric.Metrics) {
	if metrics.MetricCount() == 0 {
		return
	}

	jsonBytes, err := pmetric.NewJSONMarshaler().MarshalMetrics(metrics)
	if err != nil {
		logger.Debugf("marshal pdata metrics failed: %v", err)
		return
	}

	logger.Debugf("conversionResult.metrics=%s", string(jsonBytes))
}

// publishConvertedRecords 按 conversionResult 分流发布 logs、traces 和 metrics。
func (s HttpService) publishConvertedRecords(conversionResult ConversionResult, ip string, token string, bodySize int, start time.Time) {
	logger.Infof(
		"Converted pdata result: logs=%d spans=%d metrics=%d",
		conversionResult.Logs.LogRecordCount(),
		conversionResult.Traces.SpanCount(),
		conversionResult.Metrics.MetricCount(),
	)
	debugLogConversionResult(conversionResult)

	for _, item := range splitConversionResult(conversionResult) {
		r := &define.Record{
			RequestType:   define.RequestHttp,
			RequestClient: define.RequestClient{IP: ip},
			RecordType:    item.rtype,
			Data:          item.data,
			Token:         define.Token{Original: token},
		}

		code, processorName, err := s.Validate(r)
		if err != nil {
			err = errors.Wrapf(err, "run pre-check failed, rtype=%s, code=%d, ip=%s", item.rtype.S(), code, ip)
			logger.WarnRate(time.Minute, r.Token.Original, err)
			metricMonitor.IncPreCheckFailedCounter(define.RequestHttp, item.rtype, processorName, r.Token.Original, code)
			continue
		}

		// s.Publish(r)
		receiver.RecordHandleMetrics(metricMonitor, r.Token, define.RequestHttp, item.rtype, bodySize, start)
	}
}

func (s HttpService) RumV1(w http.ResponseWriter, req *http.Request) {
	defer utils.HandleCrash()
	ip := utils.ParseRequestIP(req.RemoteAddr, req.Header)

	start := time.Now()
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, req.Body)
	if err != nil {
		metricMonitor.IncInternalErrorCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteResponse(w, define.ContentTypeJson, http.StatusInternalServerError, nil)
		logger.Errorf("failed to read datadog rum body: %v", err)
		return
	}
	defer func() {
		_ = req.Body.Close()
	}()

	// 记录接收到的原始数据
	dataBytes := buf.Bytes()
	logger.Infof("RumV1: received %d bytes data: %s", len(dataBytes), string(dataBytes[:min(len(dataBytes), 500)]))

	records, err := parseDatadogRUM(dataBytes)
	if err != nil {
		logger.Warnf("failed to parse datadog rum exported content, ip=%v, err: %v", ip, err)
		metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteErrResponse(w, define.ContentTypeJson, http.StatusBadRequest, err)
		return
	}

	token := tokenparser.FromHttpRequest(req)

	for idx, data := range records {
		logger.Warnf("RumV1: processing record %d, data type=%T", idx, data)
		if m, ok := data.(map[string]interface{}); ok {
			logger.Warnf("RumV1: record is map with %d keys", len(m))
		}

		// 转换数据为 OTEL 格式
		conversionResult, err := transformRecord(data)
		if err != nil {
			logger.Warnf("RumV1: failed to transform record %d: %v", idx, err)
			metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
			continue
		}
		// 打印 josn 内容
		s.publishConvertedRecords(conversionResult, ip, token, buf.Len(), start)
	}

	ddRequestID := req.URL.Query().Get("dd-request-id")
	response := map[string]string{"request_id": ddRequestID}
	responseBuf, _ := json.Marshal(response)
	receiver.WriteResponse(w, define.ContentTypeJson, http.StatusOK, responseBuf)
}

func (s HttpService) RumV2(w http.ResponseWriter, req *http.Request) {
	defer utils.HandleCrash()
	ip := utils.ParseRequestIP(req.RemoteAddr, req.Header)

	start := time.Now()
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, req.Body)
	if err != nil {
		metricMonitor.IncInternalErrorCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteResponse(w, define.ContentTypeJson, http.StatusInternalServerError, nil)
		logger.Errorf("failed to read datadog rum v2 body: %v", err)
		return
	}
	defer func() {
		_ = req.Body.Close()
	}()

	// 记录接收到的原始数据
	dataBytes := buf.Bytes()
	logger.Infof("RumV2: received %d bytes data: %s", len(dataBytes), string(dataBytes[:min(len(dataBytes), 500)]))

	records, err := parseDatadogRUMV2(dataBytes)
	if err != nil {
		logger.Warnf("failed to parse datadog rum v2 exported content, ip=%v, err: %v", ip, err)
		metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
		receiver.WriteErrResponse(w, define.ContentTypeJson, http.StatusBadRequest, err)
		return
	}

	token := tokenparser.FromHttpRequest(req)

	for idx, data := range records {
		logger.Warnf("RumV2: processing record %d, data type=%T", idx, data)
		if m, ok := data.(map[string]interface{}); ok {
			logger.Warnf("RumV2: record is map with %d keys", len(m))
		}

		// 转换数据为 OTEL 格式
		conversionResult, err := transformRecord(data)
		if err != nil {
			logger.Warnf("RumV2: failed to transform record %d: %v", idx, err)
			metricMonitor.IncDroppedCounter(define.RequestHttp, define.RecordLogs)
			continue
		}

		s.publishConvertedRecords(conversionResult, ip, token, buf.Len(), start)
	}

	ddRequestID := req.URL.Query().Get("dd-request-id")
	response := map[string]string{"request_id": ddRequestID}
	responseBuf, _ := json.Marshal(response)
	receiver.WriteResponse(w, define.ContentTypeJson, http.StatusOK, responseBuf)
}
