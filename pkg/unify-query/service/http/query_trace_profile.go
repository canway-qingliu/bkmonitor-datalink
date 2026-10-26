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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/dorisclient"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/http/middleware"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

// 未配置直连读取时的固定拒绝文案，与已上线的 fail-closed 行为保持一致。
const traceProfileUnwiredMessage = "Trace profile 直连存储查询入口尚未完成"

// 发布源尚未成功读取或本地配置非法时的拒绝文案。
const traceProfileUnavailableMessage = "Trace 读目标暂不可用"

// 发布源读取成功、但没有可服务的读目标（未发布或全部租户被撤销）时的拒绝文案。
const traceProfilePublishedEmptyMessage = "Trace 读目标尚未发布或已撤销"

var errTraceProfileReadTargetsUnavailable = errors.New("Trace 读目标不可用")

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
	AppID               int64  `json:"app_id,omitempty"`
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
	// EntryIndexVersion 是发布方声明的入口索引版本；2 时跨应用父关系筛选与逐行复核才生效。
	EntryIndexVersion int64 `json:"entry_index_version,omitempty"`
	PublishedVersion  int64 `json:"published_route_version,omitempty"`
}

type traceProfileApplicationTopologyTargetConfig struct {
	BkTenantID       string `json:"bk_tenant_id"`
	Label            string `json:"label"`
	Database         string `json:"database"`
	Table            string `json:"table"`
	ClusterID        string `json:"cluster_id"`
	ClusterName      string `json:"cluster_name"`
	SchemaVersion    string `json:"schema_version"`
	RetentionDays    int64  `json:"retention_days"`
	PublishedVersion int64  `json:"published_route_version"`
}

func normalizeApplicationTopologyTargetConfigs(
	legacy, descriptive []traceProfileApplicationTopologyTargetConfig,
) ([]traceProfileApplicationTopologyTargetConfig, error) {
	if legacy == nil && descriptive == nil {
		return nil, nil
	}
	legacyNormalized := make([]traceProfileApplicationTopologyTargetConfig, 0, len(legacy))
	legacyByKey := make(map[[2]string]traceProfileApplicationTopologyTargetConfig, len(legacy))
	legacyLabels := make(map[string]uint8)
	for _, target := range legacy {
		switch target.Label {
		case "a3_edge":
			target.Label = "application_edge"
		case "a3_coverage":
			target.Label = "application_coverage"
		default:
			return nil, fmt.Errorf("旧版应用拓扑读目标标签非法")
		}
		key := [2]string{target.BkTenantID, target.Label}
		if _, exists := legacyByKey[key]; exists {
			return nil, fmt.Errorf("旧版应用拓扑读目标重复")
		}
		legacyByKey[key] = target
		if target.Label == "application_edge" {
			legacyLabels[target.BkTenantID] |= 1
		} else {
			legacyLabels[target.BkTenantID] |= 2
		}
		legacyNormalized = append(legacyNormalized, target)
	}
	for _, labels := range legacyLabels {
		if labels != 3 {
			return nil, fmt.Errorf("旧版应用拓扑边与覆盖读目标必须成对发布")
		}
	}
	if descriptive == nil {
		return legacyNormalized, nil
	}
	merged := make([]traceProfileApplicationTopologyTargetConfig, 0, len(legacyNormalized)+len(descriptive))
	seen := make(map[[2]string]bool, len(descriptive))
	for _, target := range descriptive {
		if target.Label != "application_edge" && target.Label != "application_coverage" {
			return nil, fmt.Errorf("应用拓扑读目标标签非法")
		}
		key := [2]string{target.BkTenantID, target.Label}
		if seen[key] {
			return nil, fmt.Errorf("应用拓扑读目标重复")
		}
		if previous, exists := legacyByKey[key]; exists && previous != target {
			return nil, fmt.Errorf("应用拓扑新旧读目标不一致")
		}
		seen[key] = true
		merged = append(merged, target)
	}
	for _, target := range legacyNormalized {
		if !seen[[2]string{target.BkTenantID, target.Label}] {
			merged = append(merged, target)
		}
	}
	return merged, nil
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
	RouteSnapshotVersion       int64                                         `json:"route_snapshot_version"`
	ReadTargetDataPath         string                                        `json:"read_target_data_path,omitempty"`
	ReadTargetVersionPath      string                                        `json:"read_target_version_path,omitempty"`
	MaxBodyBytes               int                                           `json:"max_body_bytes"`
	MaxLimit                   int64                                         `json:"max_limit"`
	MaxOffset                  int64                                         `json:"max_offset"`
	MaxRangeDays               int64                                         `json:"max_range_days"`
	ReadRoutes                 []traceProfileRouteConfig                     `json:"read_routes"`
	ESSummaryTargets           []traceProfileESTargetConfig                  `json:"es_summary_targets,omitempty"`
	LegacyA3Targets            []traceProfileApplicationTopologyTargetConfig `json:"a3_targets,omitempty"`
	ApplicationTopologyTargets []traceProfileApplicationTopologyTargetConfig `json:"application_topology_targets,omitempty"`
	ReadonlyConnections        []traceProfileConnectionConfig                `json:"readonly_connections"`

	// 单租户 typed Trace 的真实数据租户；应用和空间权限沿用 JWT 鉴权。
	// 配置非法时加载即失败；未配置且读目标快照恰好只有一个租户时取该租户，
	// 否则沿用入站租户。不从请求参数猜测数据归属。
	SingleTenantID                string   `json:"single_tenant_id,omitempty"`
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
	maxRangeUS     int64
	esLimits       traceprofile.ESSummaryLimits
	routeVersion   int64
	routeCount     int
	// published 与 configDigest 仅发布源模式使用：configDigest 是构建该快照时的本地输入摘要
	// （traceProfileConfigDigest）。
	published    bool
	configDigest [sha256.Size]byte

	// 模式和数据租户在同一快照内，避免配置热更新时混用新旧值。
	singleTenantID              string
	multiTenantMode             bool
	allowedAppCodes             []string
	memberLookupAllowedAppCodes []string
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

// loadTraceProfileConfig 由 http 配置加载钩子调用。本地配置变化立即生效：静态模式按配置重建，
// 发布源模式用缓存的发布快照重建；静态配置非法或发布源变化时关闭直读而不是降级。
func loadTraceProfileConfig() {
	if traceProfileUsesPublishedRoutes() {
		applyTraceProfilePublishedConfig()
		return
	}
	applyTraceProfileStaticConfig()
}

// newTraceProfileRuntime 按静态配置构建运行时；单租户映射从配置自身的读目标推导。
func newTraceProfileRuntime(raw any) (*traceProfileRuntime, error) {
	return newTraceProfileRuntimeFor(raw, nil)
}

// newTraceProfileRuntimeFor 构建运行时。publishedTenants 非 nil 时是发布源上出现的全部租户，
// 单租户映射按它推导，而不是只按校验通过的租户。
func newTraceProfileRuntimeFor(raw any, publishedTenants map[string]struct{}) (*traceProfileRuntime, error) {
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
	topologyTargetConfigs, err := normalizeApplicationTopologyTargetConfigs(config.LegacyA3Targets, config.ApplicationTopologyTargets)
	if err != nil {
		return nil, err
	}
	if len(config.ReadRoutes) == 0 && len(config.ReadonlyConnections) == 0 &&
		len(config.ESSummaryTargets) == 0 && len(topologyTargetConfigs) == 0 {
		return nil, nil
	}
	if (len(config.ReadRoutes) == 0 && len(topologyTargetConfigs) == 0) != (len(config.ReadonlyConnections) == 0) {
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
		var target traceprofile.DorisTarget
		if route.AppID > 0 {
			// 应用 Span 的物理名只来自已校验发布物，快照构造还会逐项验证应用 ID 与表名。
			target = traceprofile.DorisTarget{
				ClusterID: route.ClusterID, Database: route.Database,
				Table: route.Table, SchemaVersion: route.SchemaVersion,
			}
		} else {
			var err error
			target, err = traceprofile.NewDorisTarget(
				route.ClusterID, route.ObservationDomainID, route.BkBizID, profile, route.SchemaVersion,
			)
			if err != nil {
				closeConnections()
				return nil, err
			}
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
			AppID:               route.AppID,
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
			EntryIndexVersion:   target.EntryIndexVersion,
			PublishedVersion:    target.PublishedVersion,
		})
	}
	topologyTargets := make([]traceprofile.ApplicationTopologyTarget, 0, len(topologyTargetConfigs))
	for _, target := range topologyTargetConfigs {
		if _, connected := connections[target.ClusterID]; !connected {
			closeConnections()
			return nil, fmt.Errorf("应用拓扑 读目标的集群缺少只读连接")
		}
		topologyTargets = append(topologyTargets, traceprofile.ApplicationTopologyTarget{
			BkTenantID: target.BkTenantID, Label: target.Label,
			Database: target.Database, Table: target.Table, ClusterID: target.ClusterID,
			ClusterName: target.ClusterName, SchemaVersion: target.SchemaVersion,
			RetentionDays: target.RetentionDays, PublishedVersion: target.PublishedVersion,
		})
	}
	snapshot, err := traceprofile.NewRouteSnapshotWithAllTargets(config.RouteSnapshotVersion, routes, esTargets, topologyTargets)
	if err != nil {
		closeConnections()
		return nil, err
	}
	singleTenantID, err := traceProfileSingleTenantID(config.SingleTenantID, routes, esTargets, topologyTargets, publishedTenants)
	if err != nil {
		closeConnections()
		return nil, err
	}

	queriers := make(map[string]traceprofile.Querier, len(connections))
	for clusterID, instance := range connections {
		queriers[clusterID] = instance
	}
	executor, err := traceprofile.NewExecutor(snapshot, queriers, traceprofile.Limits{
		MaxRangeUS: traceprofile.MaxWindowUS(config.MaxRangeDays),
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
		maxRangeUS:     traceprofile.MaxWindowUS(config.MaxRangeDays),
		esLimits:       traceprofile.NewESSummaryLimits(config.MaxLimit, config.MaxOffset),
		routeVersion:   config.RouteSnapshotVersion,
		routeCount:     len(routes),

		singleTenantID:              singleTenantID,
		multiTenantMode:             viper.GetBool(influxdb.MultiTenantModeConfigPath),
		allowedAppCodes:             config.AllowedBkAppCodes,
		memberLookupAllowedAppCodes: config.MemberLookupAllowedBkAppCodes,
	}, nil
}

// traceProfileSingleTenantID 是单租户网关请求（入站租户为 default）映射到的数据租户：
// 配置值必须是合法租户标识，否则加载失败；未配置时，读目标恰好只有一个租户就取它。
// publishedTenants 非 nil 时按发布源上的全部租户计数，一个租户被撤销不会改变映射。
func traceProfileSingleTenantID(
	configured string, routes []traceprofile.ReadRoute, esTargets []traceprofile.ESSummaryTarget,
	topologyTargets []traceprofile.ApplicationTopologyTarget,
	publishedTenants map[string]struct{},
) (string, error) {
	if configured != "" {
		if !traceprofile.ValidTenantID(configured) {
			return "", fmt.Errorf("Trace single_tenant_id 不是合法租户标识: %q", configured)
		}
		return configured, nil
	}
	tenants := publishedTenants
	if tenants == nil {
		tenants = make(map[string]struct{}, 1)
		for _, route := range routes {
			tenants[route.BkTenantID] = struct{}{}
		}
		for _, target := range esTargets {
			tenants[target.BkTenantID] = struct{}{}
		}
		for _, target := range topologyTargets {
			tenants[target.BkTenantID] = struct{}{}
		}
	}
	if len(tenants) != 1 {
		return "", nil
	}
	for tenant := range tenants {
		return tenant, nil
	}
	return "", nil
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
				case "trace_profile", "trace_contract_version", "trace_request", "a3_contract_version", "a3_request",
					"application_topology_contract_version", "application_topology_request":
					return true
				}
			}
		}
		if depth == 1 {
			expectKey = !expectKey
		}
	}
}

// handleTraceProfileQuery 返回非 nil 时表示后端失败，调用方据此把请求 span 记为失败；
// 请求被拒（4xx）不算 span 失败，已由拒绝日志与指标记录。
func handleTraceProfileQuery(c *gin.Context, ctx context.Context, span *trace.Span) error {
	runtime := traceProfileRuntimeValue.Load()
	if runtime == nil {
		switch reason := traceProfileRuntimeClosedReason(); reason {
		case traceRuntimeUnavailable, traceRuntimeInvalidConfig:
			// 发布源还没有成功读取，或本地配置非法：服务端不可用，不是请求错误。
			return writeTraceProfileError(ctx, c, span, &traceprofile.RejectError{
				Code: traceprofile.CodeRouteNotFound, Message: traceProfileUnavailableMessage,
				Cause: fmt.Errorf("%w: %s", errTraceProfileReadTargetsUnavailable, reason),
			})
		case traceRuntimeUnpublished, traceRuntimeRevoked:
			return writeTraceProfileReject(ctx, c, span, traceprofile.CodeRouteNotFound, traceProfilePublishedEmptyMessage)
		}
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeRouteNotFound, traceProfileUnwiredMessage)
	}
	appCode, verified := middleware.VerifiedJWTAppCode(c)
	if err := authorizeTraceProfile(verified, appCode, runtime.allowedAppCodes); err != nil {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnauthorizedMember, err.Error())
	}

	body, err := io.ReadAll(io.LimitReader(c.Request.Body, int64(runtime.maxBodyBytes)+1))
	if err != nil {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnsupportedSchemaCapability, "Trace profile 请求体读取失败")
	}
	if isApplicationTopologyRangeEnvelope(body) {
		return handleApplicationTopologyRangeQuery(c, ctx, span, runtime, verified, appCode, body)
	}
	request, err := traceprofile.ParseRequest(body, runtime.maxBodyBytes, runtime.maxLimit)
	if err != nil {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnsupportedSchemaCapability, err.Error())
	}
	// 时间预算属于 typed 合同，Doris 与 ES 成员读取必须在分流前一致校验。
	if !traceprofile.WindowFits(request.StartUS, request.EndUS, runtime.maxRangeUS) {
		return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnboundedRange, "Trace 查询时间范围超过预算")
	}
	if request.MemberLookup != nil || request.ESSummaryList != nil {
		if err := authorizeTraceProfile(verified, appCode, runtime.memberLookupAllowedAppCodes); err != nil {
			return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnauthorizedMember, err.Error())
		}
	}
	user := metadata.GetUser(ctx)
	dataTenantID := user.TenantID
	// 单租户 APIGW 把租户头规范化为 default，这是传输协议值。
	// JWT 应用和空间已鉴权；只按环境配置找数据租户，不从请求体猜租户，
	// 映射结果仍须通过下方的作用域与发布路由校验。
	if !runtime.multiTenantMode && dataTenantID == "default" && runtime.singleTenantID != "" {
		dataTenantID = runtime.singleTenantID
	}
	span.Set("trace-profile-request-tenant-id", user.TenantID)
	span.Set("trace-profile-data-tenant-id", dataTenantID)
	scope, err := traceprofile.ResolveScope(dataTenantID, user.SpaceUID, request.ObservationDomainID)
	if err != nil {
		return writeTraceProfileError(ctx, c, span, err)
	}
	isESMember := request.MemberLookup != nil && request.MemberLookup.Backend == traceprofile.MemberBackendElasticsearch
	isESList := request.ESSummaryList != nil
	var esTarget traceprofile.ESSummaryTarget
	if isESMember || isESList {
		pair := traceprofile.MemberPair{}
		version := int64(0)
		if isESMember {
			pair = request.MemberLookup.AllowedPairs[0]
			version = request.MemberLookup.ExpectedRouteVersion
		} else {
			pair = traceprofile.MemberPair{AppID: request.ESSummaryList.AppID, BkBizID: request.ESSummaryList.BkBizID}
			version = request.ESSummaryList.ExpectedRouteVersion
		}
		if pair.BkBizID != scope.BkBizID {
			return writeTraceProfileReject(ctx, c, span, traceprofile.CodeUnauthorizedMember, "ES Summary 成员业务不在受信空间内")
		}
		target, targetErr := runtime.executor.ResolveESSummaryTarget(
			scope.BkTenantID, pair.BkBizID, pair.AppID, scope.ObservationDomainID,
		)
		if targetErr != nil {
			return writeTraceProfileError(ctx, c, span, targetErr)
		}
		if target.PublishedVersion == 0 || target.PublishedVersion != version {
			return writeTraceProfileReject(ctx, c, span, traceprofile.CodeScopePartial, "ES Summary 单租户发布版本尚未在 UQ 生效")
		}
		esTarget = target
	}

	span.Set("trace-profile", string(request.Profile))
	span.Set("trace-profile-route-version", runtime.routeVersion)
	if request.Aggregation != nil {
		rows, aggregateErr := runtime.executor.ExecuteAggregate(ctx, request, scope)
		if aggregateErr != nil {
			return writeTraceProfileError(ctx, c, span, aggregateErr)
		}
		span.Set("trace-profile-aggregate-points", len(rows))
		metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusSuccess, user.SpaceUID, user.Source)
		c.JSON(http.StatusOK, traceprofile.NewAggregateResponse(request, rows))
		return nil
	}
	var page traceprofile.Page
	if isESMember {
		page, err = executeESMemberLookup(ctx, request, scope, esTarget, runtime.esMemberReader, runtime.esLimits)
	} else if isESList {
		page, err = executeESList(ctx, request, scope, esTarget, runtime.esMemberReader, runtime.esLimits)
	} else {
		page, err = runtime.executor.Execute(ctx, request, scope)
	}
	if err != nil {
		return writeTraceProfileError(ctx, c, span, err)
	}
	span.Set("trace-profile-total", page.Total)

	metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusSuccess, user.SpaceUID, user.Source)
	c.JSON(http.StatusOK, traceprofile.NewResponse(request, page))
	return nil
}

type traceProfileErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeTraceProfileReject 以封闭错误码拒绝请求。
func writeTraceProfileReject(ctx context.Context, c *gin.Context, span *trace.Span, code, message string) error {
	return writeTraceProfileError(ctx, c, span, &traceprofile.RejectError{Code: code, Message: message})
}

// writeTraceProfileError 写出封闭错误体，并记录日志、按错误码计数、标记 span：
//
//   - 后端失败（存储读取、结果复核）返回 502，读目标不可用返回 503，其余为 400；
//   - 响应体与 span 只带封闭错误码与文案；后端原因（可能含驱动错误原文与后端地址）
//     只写服务端日志。
func writeTraceProfileError(ctx context.Context, c *gin.Context, span *trace.Span, err error) error {
	code := traceprofile.ErrorCode(err)
	// 只回封闭文案：未分类错误不回原文。
	message := "Trace 查询失败"
	var rejected *traceprofile.RejectError
	if errors.As(err, &rejected) {
		message = rejected.Message
	}
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, errTraceProfileReadTargetsUnavailable):
		status = http.StatusServiceUnavailable
	case traceprofile.IsBackendFailure(code):
		status = http.StatusBadGateway
	}
	metricCode := code
	if !traceprofile.IsKnownCode(metricCode) {
		metricCode = "other"
	}
	user := metadata.GetUser(ctx)
	metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusFailed, user.SpaceUID, user.Source)
	metric.TraceProfileRejection(metricCode, status)
	if span != nil {
		span.Set("trace-profile-error-code", code)
		span.Set("trace-profile-http-status", status)
	}
	if cause := traceprofile.RejectCause(err); status >= http.StatusInternalServerError {
		log.Errorf(ctx, "trace profile request failed: code=%s status=%d tenant=%s space=%s message=%s cause=%v",
			code, status, user.TenantID, user.SpaceUID, message, cause)
	} else {
		log.Warnf(ctx, "trace profile request rejected: code=%s status=%d tenant=%s space=%s message=%s",
			code, status, user.TenantID, user.SpaceUID, message)
	}
	// 错误体同样是封闭合同：只有 code 与 message。
	c.JSON(status, traceProfileErrorResponse{Code: code, Message: message})
	if status >= http.StatusInternalServerError {
		return fmt.Errorf("%s: %s", code, message)
	}
	return nil
}
