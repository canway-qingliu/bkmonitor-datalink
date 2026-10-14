// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/http/middleware"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

// 未配置直连读取时的固定拒绝文案，与已上线的 fail-closed 行为保持一致。
const traceProfileUnwiredMessage = "Trace profile 直连存储查询入口尚未完成"

const (
	defaultTraceProfileMaxBodyBytes = 65536
	defaultTraceProfileMaxLimit     = 10000
	defaultTraceProfileMaxOffset    = 100000
	defaultTraceProfileMaxRangeDays = 30
	// traceProfileProbeBytes 只用于识别请求是否携带 typed Trace 信封，非 Trace 请求会原样回填。
	traceProfileProbeBytes = 4096
)

type traceProfileRouteConfig struct {
	BkTenantID          string `json:"bk_tenant_id"`
	BkBizID             int64  `json:"bk_biz_id"`
	Profile             string `json:"profile"`
	ObservationDomainID string `json:"observation_domain_id"`
	Database            string `json:"database,omitempty"`
	Table               string `json:"table,omitempty"`
	ClusterID           string `json:"cluster_id"`
	ClusterName         string `json:"cluster_name,omitempty"`
	SchemaVersion       string `json:"schema_version"`
	PublishedVersion    int64  `json:"published_route_version,omitempty"`
}

type traceProfileESTargetConfig struct {
	BkTenantID          string `json:"bk_tenant_id"`
	BkBizID             int64  `json:"bk_biz_id"`
	AppID               int64  `json:"app_id"`
	AppName             string `json:"app_name"`
	ObservationDomainID string `json:"observation_domain_id"`
	ResultTableID       string `json:"read_result_table_id"`
	StorageClusterID    int64  `json:"storage_cluster_id"`
	RetentionDays       int64  `json:"retention_days"`
	PublishedVersion    int64  `json:"published_route_version,omitempty"`
}

type traceProfileConnectionConfig struct {
	ClusterID      string       `json:"cluster_id"`
	Host           string       `json:"host"`
	Port           int          `json:"port"`
	Username       string       `json:"username"`
	Password       string       `json:"password"`
	MaxOpenConns   int          `json:"max_open_conns"`
	ConnectTimeout jsonDuration `json:"connect_timeout"`
	QueryTimeout   jsonDuration `json:"query_timeout"`
	MaxRows        int          `json:"max_rows"`
}

type traceProfileConfig struct {
	RouteSnapshotVersion  int64                          `json:"route_snapshot_version"`
	ReadTargetDataPath    string                         `json:"read_target_data_path,omitempty"`
	ReadTargetVersionPath string                         `json:"read_target_version_path,omitempty"`
	MaxBodyBytes          int                            `json:"max_body_bytes"`
	MaxLimit              int64                          `json:"max_limit"`
	MaxOffset             int64                          `json:"max_offset"`
	MaxRangeDays          int64                          `json:"max_range_days"`
	ReadRoutes            []traceProfileRouteConfig      `json:"read_routes"`
	ESSummaryTargets      []traceProfileESTargetConfig   `json:"es_summary_targets,omitempty"`
	ReadonlyConnections   []traceProfileConnectionConfig `json:"readonly_connections"`
	// 该键由 settings.TraceProfileAllowedAppCodes 负责，这里只为其保留严格解码入口。
	AllowedBkAppCodes             []string `json:"allowed_bk_app_codes"`
	MemberLookupAllowedBkAppCodes []string `json:"member_lookup_allowed_bk_app_codes"`
}

type jsonDuration struct {
	time.Duration
}

func (d *jsonDuration) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return fmt.Errorf("时长必须写成字符串（如 10s）: %w", err)
	}
	if text == "" {
		return nil
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

// traceProfileRuntime 是直连读取的全部运行时状态：可信路由快照、只读连接与预算。
// 配置未提供或非法时保持 nil，请求一律 fail closed。
type traceProfileRuntime struct {
	executor       *traceprofile.Executor
	esMemberReader esMemberRawReader
	connections    map[string]*dorisclient.Instance
	maxBodyBytes   int
	maxLimit       int64
	routeVersion   int64
	routeCount     int
}

func traceProfileMaxRangeUS(days int64) int64 {
	// Kingeye 产品窗口的毫秒结束点是含端点，typed 读窗口为排他微秒终点。
	return days*24*int64(time.Hour/time.Microsecond) + 1000
}

func (r *traceProfileRuntime) close() {
	for _, connection := range r.connections {
		if err := connection.Close(); err != nil {
			log.Warnf(context.Background(), "close trace profile readonly connection failed: %s", err)
		}
	}
}

var traceProfileRuntimeValue atomic.Pointer[traceProfileRuntime]

func swapTraceProfileRuntime(runtime *traceProfileRuntime) {
	previous := traceProfileRuntimeValue.Swap(runtime)
	if previous == nil {
		return
	}
	// 热更新时给在途查询留出收尾时间，再释放旧连接。
	time.AfterFunc(time.Minute, previous.close)
}

// loadTraceProfileConfig 由 http 配置加载钩子调用；任何一步失败都关闭直连读取而不是降级。
func loadTraceProfileConfig() {
	if traceProfileUsesConsul() {
		// Consul 实例由服务启动时建立；配置解析阶段先关闭直读，随后加载发布快照。
		swapTraceProfileRuntime(nil)
		return
	}
	runtime, err := newTraceProfileRuntime(viper.Get(TraceProfileConfigPath))
	if err != nil {
		log.Errorf(context.Background(), "load trace profile direct read config failed: %s", err)
		swapTraceProfileRuntime(nil)
		return
	}
	swapTraceProfileRuntime(runtime)
	if runtime == nil {
		return
	}
	log.Infof(
		context.Background(), "trace profile direct read enabled: route-version=%d routes=%d clusters=%d",
		runtime.routeVersion, runtime.routeCount, len(runtime.connections),
	)
}

func newTraceProfileRuntime(raw any) (*traceProfileRuntime, error) {
	if raw == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("Trace profile 配置不可序列化: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	config := traceProfileConfig{
		RouteSnapshotVersion: 1,
		MaxBodyBytes:         defaultTraceProfileMaxBodyBytes,
		MaxLimit:             defaultTraceProfileMaxLimit,
		MaxOffset:            defaultTraceProfileMaxOffset,
		MaxRangeDays:         defaultTraceProfileMaxRangeDays,
	}
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("Trace profile 直连读取配置非法: %w", err)
	}
	if len(config.ReadRoutes) == 0 && len(config.ReadonlyConnections) == 0 && len(config.ESSummaryTargets) == 0 {
		return nil, nil
	}
	if (len(config.ReadRoutes) == 0) != (len(config.ReadonlyConnections) == 0) {
		return nil, fmt.Errorf("Trace profile 直连读取必须同时提供读路由与只读连接")
	}
	if config.MaxBodyBytes <= 0 || config.MaxLimit <= 0 || config.MaxOffset <= 0 ||
		config.MaxRangeDays <= 0 || config.MaxRangeDays > 30 {
		return nil, fmt.Errorf("Trace profile 直连读取预算必须为正数")
	}

	connections := make(map[string]*dorisclient.Instance, len(config.ReadonlyConnections))
	closeConnections := func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}
	for _, connection := range config.ReadonlyConnections {
		if _, duplicated := connections[connection.ClusterID]; duplicated {
			closeConnections()
			return nil, fmt.Errorf("Trace 只读连接的集群重复: %s", connection.ClusterID)
		}
		instance, err := dorisclient.New(dorisclient.Options{
			ClusterID:      connection.ClusterID,
			Host:           connection.Host,
			Port:           connection.Port,
			Username:       connection.Username,
			Password:       connection.Password,
			MaxOpenConns:   connection.MaxOpenConns,
			ConnectTimeout: connection.ConnectTimeout.Duration,
			QueryTimeout:   connection.QueryTimeout.Duration,
			MaxRows:        connection.MaxRows,
		})
		if err != nil {
			closeConnections()
			return nil, err
		}
		connections[connection.ClusterID] = instance
	}

	routes := make([]traceprofile.ReadRoute, 0, len(config.ReadRoutes))
	for _, route := range config.ReadRoutes {
		profile, err := traceprofile.ParseProfile(route.Profile)
		if err != nil {
			closeConnections()
			return nil, err
		}
		// 物理库/表由冻结命名派生，配置只能提供集群与 schema 版本。
		target, err := traceprofile.NewDorisTarget(
			route.ClusterID, route.ObservationDomainID, route.BkBizID, profile, route.SchemaVersion,
		)
		if err != nil {
			closeConnections()
			return nil, err
		}
		if route.Database != "" && route.Database != target.Database || route.Table != "" && route.Table != target.Table {
			closeConnections()
			return nil, fmt.Errorf("Trace 发布路由物理库表与受信业务命名不匹配")
		}
		// 路由所指集群必须有可用的只读连接，否则配置期就拒绝而不是查询期才失败。
		if _, connected := connections[route.ClusterID]; !connected {
			closeConnections()
			return nil, fmt.Errorf("Trace 读路由的集群缺少只读连接: %s", route.ClusterID)
		}
		routes = append(routes, traceprofile.ReadRoute{
			BkTenantID:          route.BkTenantID,
			BkBizID:             route.BkBizID,
			Profile:             profile,
			ObservationDomainID: route.ObservationDomainID,
			PublishedVersion:    route.PublishedVersion,
			Doris:               &target,
		})
	}
	esTargets := make([]traceprofile.ESSummaryTarget, 0, len(config.ESSummaryTargets))
	for _, target := range config.ESSummaryTargets {
		esTargets = append(esTargets, traceprofile.ESSummaryTarget{
			BkTenantID: target.BkTenantID, BkBizID: target.BkBizID,
			AppID: target.AppID, AppName: target.AppName,
			ObservationDomainID: target.ObservationDomainID,
			ResultTableID:       target.ResultTableID,
			StorageClusterID:    target.StorageClusterID,
			RetentionDays:       target.RetentionDays,
			PublishedVersion:    target.PublishedVersion,
		})
	}
	snapshot, err := traceprofile.NewRouteSnapshotWithESTargets(config.RouteSnapshotVersion, routes, esTargets)
	if err != nil {
		closeConnections()
		return nil, err
	}

	queriers := make(map[string]traceprofile.Querier, len(connections))
	for clusterID, instance := range connections {
		queriers[clusterID] = instance
	}
	executor, err := traceprofile.NewExecutor(snapshot, queriers, traceprofile.Limits{
		MaxRangeUS: traceProfileMaxRangeUS(config.MaxRangeDays),
		MaxOffset:  config.MaxOffset,
	})
	if err != nil {
		closeConnections()
		return nil, err
	}
	return &traceProfileRuntime{
		executor:       executor,
		esMemberReader: queryESMemberRawWithTimeField,
		connections:    connections,
		maxBodyBytes:   config.MaxBodyBytes,
		maxLimit:       config.MaxLimit,
		routeVersion:   config.RouteSnapshotVersion,
		routeCount:     len(routes),
	}, nil
}

// probeTraceEnvelope 只读取前缀用于识别 typed Trace 信封；非 Trace 请求的 body 原样回填，
// 老链路的解码行为不变。前缀扫描漏掉的转义键仍会被既有的 HasTraceEnvelope 校验拦下。
func probeTraceEnvelope(c *gin.Context) bool {
	if c == nil || c.Request == nil || c.Request.Body == nil {
		return false
	}
	probe, err := io.ReadAll(io.LimitReader(c.Request.Body, traceProfileProbeBytes))
	c.Request.Body = io.NopCloser(io.MultiReader(bytes.NewReader(probe), c.Request.Body))
	if err != nil || len(probe) == 0 {
		return false
	}
	return prefixHasTraceEnvelope(probe)
}

func prefixHasTraceEnvelope(probe []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(probe))
	depth := 0
	expectKey := false
	for {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		if delim, isDelim := token.(json.Delim); isDelim {
			switch delim {
			case '{', '[':
				depth++
				expectKey = depth == 1 && delim == '{'
			case '}', ']':
				depth--
			}
			continue
		}
		if depth == 1 && expectKey {
			if key, isKey := token.(string); isKey {
				switch key {
				case "trace_profile", "trace_contract_version", "trace_request":
					return true
				}
			}
		}
		if depth == 1 {
			expectKey = !expectKey
		}
	}
}

func handleTraceProfileQuery(c *gin.Context, ctx context.Context, span *trace.Span) {
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil {
		writeTraceProfileReject(ctx, c, traceprofile.CodeRouteNotFound, traceProfileUnwiredMessage)
		return
	}
	appCode, verified := middleware.VerifiedJWTAppCode(c)
	if err := authorizeTraceProfile(verified, appCode, TraceProfileAllowedAppCodes); err != nil {
		writeTraceProfileReject(ctx, c, traceprofile.CodeUnauthorizedMember, err.Error())
		return
	}

	body, err := io.ReadAll(io.LimitReader(c.Request.Body, int64(runtime.maxBodyBytes)+1))
	if err != nil {
		writeTraceProfileReject(ctx, c, traceprofile.CodeUnsupportedSchemaCapability, "Trace profile 请求体读取失败")
		return
	}
	request, err := traceprofile.ParseRequest(body, runtime.maxBodyBytes, runtime.maxLimit)
	if err != nil {
		writeTraceProfileReject(ctx, c, traceprofile.CodeUnsupportedSchemaCapability, err.Error())
		return
	}
	if request.MemberLookup != nil {
		if err := authorizeTraceProfile(verified, appCode, TraceMemberLookupAllowedAppCodes); err != nil {
			writeTraceProfileReject(ctx, c, traceprofile.CodeUnauthorizedMember, err.Error())
			return
		}
	}
	user := metadata.GetUser(ctx)
	scope, err := traceprofile.ResolveScope(user.TenantID, user.SpaceUID, request.ObservationDomainID)
	if err != nil {
		writeTraceProfileReject(ctx, c, traceprofile.ErrorCode(err), err.Error())
		return
	}
	isESMember := request.MemberLookup != nil && request.MemberLookup.Backend == traceprofile.MemberBackendElasticsearch
	var esTarget traceprofile.ESSummaryTarget
	if isESMember {
		pair := request.MemberLookup.AllowedPairs[0]
		if pair.BkBizID != scope.BkBizID {
			writeTraceProfileReject(ctx, c, traceprofile.CodeUnauthorizedMember, "ES Summary 成员业务不在受信空间内")
			return
		}
		target, targetErr := runtime.executor.ResolveESSummaryTarget(
			scope.BkTenantID, pair.BkBizID, pair.AppID, scope.ObservationDomainID,
		)
		if targetErr != nil {
			writeTraceProfileReject(ctx, c, traceprofile.ErrorCode(targetErr), targetErr.Error())
			return
		}
		if target.PublishedVersion == 0 || target.PublishedVersion != request.MemberLookup.ExpectedRouteVersion {
			writeTraceProfileReject(ctx, c, traceprofile.CodeScopePartial, "ES Summary 单租户发布版本尚未在 UQ 生效")
			return
		}
		esTarget = target
	}

	span.Set("trace-profile", string(request.Profile))
	span.Set("trace-profile-route-version", runtime.routeVersion)
	if request.Aggregation != nil {
		rows, aggregateErr := runtime.executor.ExecuteAggregate(ctx, request, scope)
		if aggregateErr != nil {
			writeTraceProfileReject(ctx, c, traceprofile.ErrorCode(aggregateErr), aggregateErr.Error())
			return
		}
		span.Set("trace-profile-aggregate-points", len(rows))
		metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusSuccess, user.SpaceUID, user.Source)
		c.JSON(http.StatusOK, traceprofile.NewAggregateResponse(request, rows))
		return
	}
	var page traceprofile.Page
	if isESMember {
		page, err = executeESMemberLookup(ctx, request, scope, esTarget, runtime.esMemberReader)
	} else {
		page, err = runtime.executor.Execute(ctx, request, scope)
	}
	if err != nil {
		writeTraceProfileReject(ctx, c, traceprofile.ErrorCode(err), err.Error())
		return
	}
	span.Set("trace-profile-total", page.Total)

	metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusSuccess, user.SpaceUID, user.Source)
	c.JSON(http.StatusOK, traceprofile.NewResponse(request, page))
}

type traceProfileErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeTraceProfileReject(ctx context.Context, c *gin.Context, code, message string) {
	user := metadata.GetUser(ctx)
	metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusFailed, user.SpaceUID, user.Source)
	// 错误体同样是封闭合同：只有 code 与 message。
	c.JSON(http.StatusBadRequest, traceProfileErrorResponse{Code: code, Message: message})
}
