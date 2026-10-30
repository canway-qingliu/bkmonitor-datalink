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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

const (
	traceReadTargetSourceConsul = "consul"
	traceReadTargetSourceRedis  = "redis"
)

// 读目标重载结果，作为指标标签的固定枚举。
const (
	traceReloadSuccess     = "success"
	traceReloadUnchanged   = "unchanged"
	traceReloadUnpublished = "unpublished"
	// traceReloadRevoked：读取成功，但每个租户都被撤销，没有安装任何运行时。
	traceReloadRevoked       = "revoked"
	traceReloadReadFailed    = "read_failed"
	traceReloadWatchFailed   = "watch_failed"
	traceReloadInvalidConfig = "invalid_config"
)

// 单个租户被撤销的原因，作为指标标签的固定枚举。
const (
	traceDocumentInvalidPath      = "invalid_path"
	traceDocumentInvalidDocument  = "invalid_document"
	traceDocumentEmptyPublication = "empty_value"
	// traceDocumentUnknownCluster 是文档本身合法、但路由指向本地没有只读连接的 Doris 集群。
	traceDocumentUnknownCluster = "unknown_cluster"
)

// 直读关闭（运行时为 nil）的原因，决定拒绝的状态码与文案。
const (
	// traceRuntimeUnconfigured：没有配置直读，或静态配置为空。
	traceRuntimeUnconfigured = ""
	// traceRuntimeUnpublished：读取成功但发布源没有任何读目标。
	traceRuntimeUnpublished = "unpublished"
	// traceRuntimeRevoked：读取成功，但每个租户都被撤销。
	traceRuntimeRevoked = "revoked"
	// traceRuntimeUnavailable：当前发布源还没有成功读取过（启动、切换发布源后读取失败）。
	traceRuntimeUnavailable = "unavailable"
	// traceRuntimeInvalidConfig：本地 trace_profile 配置非法。
	traceRuntimeInvalidConfig = "invalid_config"
)

// errTraceReadTargetsUnpublished 表示读取成功但没有任何读目标：这是发布方的明确状态，
// 与读取失败不同，直读随之关闭。
var errTraceReadTargetsUnpublished = errors.New("Trace 读目标尚未发布")

// traceProfileReadTargetSource 是 Trace 读目标的发布源：与 Storage 一样由 storage.source
// 决定，它是部署唯一的后端选择。
func traceProfileReadTargetSource() string {
	if viper.GetString("storage.source") == traceReadTargetSourceRedis {
		return traceReadTargetSourceRedis
	}
	return traceReadTargetSourceConsul
}

func traceProfileUsesPublishedRoutes() bool {
	if traceProfileUsesRedis() {
		return viper.IsSet(TraceProfileConfigPath)
	}
	return viper.GetString(TraceProfileReadTargetDataPathConfigPath) != ""
}

func traceProfileUsesRedis() bool {
	return traceProfileReadTargetSource() == traceReadTargetSourceRedis
}

// traceProfileSourceIdentity 是当前配置的发布源身份。身份变化时缓存的发布快照不再描述
// 这个发布源，不能继续服务。
func traceProfileSourceIdentity() string {
	if traceProfileUsesRedis() {
		prefix, _, _ := traceProfileRedisPaths()
		return traceReadTargetSourceRedis + "\x00" + prefix
	}
	return traceReadTargetSourceConsul + "\x00" + viper.GetString(TraceProfileReadTargetDataPathConfigPath) +
		"\x00" + viper.GetString(TraceProfileReadTargetVersionPathConfigPath)
}

// invalidTraceDocument 是一个被撤销的租户：该租户不服务，其余租户照常加载。
type invalidTraceDocument struct {
	Key      string
	TenantID string
	Version  int64
	Reason   string
	Err      error
}

// publishedTraceSnapshot 是一次成功读取得到的读目标快照。
type publishedTraceSnapshot struct {
	Version int64
	Routes  []traceProfileRouteConfig
	Invalid []invalidTraceDocument
	// Documents 是每个合法租户文档的键与版本，供撤销日志使用。
	Documents map[string]invalidTraceDocument
}

// parsePublishedTraceTargets 逐个租户校验发布文档。非法文档只撤销它自己的租户：
// 这些文档携带授权（租户路由与成员目标），收紧后的文档校验失败时不能保留旧的、更宽的授权，
// 也不能让一个租户的坏文档拖垮其余租户。unreadable 是读到了键但值不是文档的键
// （如 Redis 中非字符串类型），按非法文档处理。快照版本覆盖每个键的完整内容，
// 内容变化而文档版本未变时同样会重建。
func parsePublishedTraceTargets(prefix string, pairs api.KVPairs, unreadable map[string]error) (publishedTraceSnapshot, error) {
	if !strings.HasSuffix(prefix, "/") && !strings.HasSuffix(prefix, ":") {
		prefix += "/"
	}
	sorted := make(api.KVPairs, 0, len(pairs)+len(unreadable))
	for _, pair := range pairs {
		// Consul 的目录键（等于前缀、值为空）是目录本身，不是一份发布。
		if pair != nil && !(pair.Key == prefix && len(pair.Value) == 0) {
			sorted = append(sorted, pair)
		}
	}
	for key := range unreadable {
		sorted = append(sorted, &api.KVPair{Key: key})
	}
	if len(sorted) == 0 {
		return publishedTraceSnapshot{}, errTraceReadTargetsUnpublished
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	snapshot := publishedTraceSnapshot{
		Routes:    make([]traceProfileRouteConfig, 0),
		Documents: make(map[string]invalidTraceDocument, len(sorted)),
	}
	hasher := sha256.New()
	for _, pair := range sorted {
		content := sha256.Sum256(pair.Value)
		_, _ = hasher.Write([]byte(pair.Key + "\x00"))
		_, _ = hasher.Write(content[:])
		tenantID := strings.TrimPrefix(pair.Key, prefix)
		invalid := invalidTraceDocument{Key: pair.Key, TenantID: tenantID, Version: publishedDocumentVersion(pair.Value)}
		var document traceprofile.PublishedDocument
		switch {
		case !strings.HasPrefix(pair.Key, prefix) || tenantID == "" || strings.Contains(tenantID, "/"):
			invalid.Reason, invalid.Err = traceDocumentInvalidPath, fmt.Errorf("Trace 读目标路径缺少单一租户")
		case unreadable[pair.Key] != nil:
			invalid.Reason, invalid.Err = traceDocumentInvalidDocument, unreadable[pair.Key]
		case len(pair.Value) == 0:
			invalid.Reason, invalid.Err = traceDocumentEmptyPublication, fmt.Errorf("Trace 读目标发布项为空")
		default:
			parsed, err := traceprofile.ParsePublishedDocument(pair.Value, tenantID)
			if err != nil {
				invalid.Reason, invalid.Err = traceDocumentInvalidDocument, err
			} else {
				document = parsed
			}
		}
		if invalid.Reason != "" {
			snapshot.Invalid = append(snapshot.Invalid, invalid)
			continue
		}
		snapshot.Documents[tenantID] = invalidTraceDocument{Key: pair.Key, TenantID: tenantID, Version: document.Version}
		for _, route := range document.Routes {
			snapshot.Routes = append(snapshot.Routes, traceProfileRouteConfig{
				BkTenantID: route.BkTenantID, BkBizID: route.BkBizID, AppID: route.AppID,
				Profile:  string(route.Profile),
				Database: route.Doris.Database, Table: route.Doris.Table,
				ClusterID: route.Doris.ClusterID, SchemaVersion: route.Doris.SchemaVersion,
				PublishedVersion: document.Version,
			})
		}
	}
	digest := hasher.Sum(nil)
	snapshot.Version = int64(binary.BigEndian.Uint64(digest[:8]) & uint64(^uint64(0)>>1))
	if snapshot.Version == 0 {
		snapshot.Version = 1
	}
	return snapshot, nil
}

// publishedDocumentVersion 只为日志尽力读出非法文档声明的版本，读不出时为 0。
func publishedDocumentVersion(body []byte) int64 {
	var header struct {
		Version int64 `json:"route_snapshot_version"`
	}
	if json.Unmarshal(body, &header) != nil {
		return 0
	}
	return header.Version
}

// withoutUnknownClusters 撤销路由指向本地没有只读连接的 Doris 集群的租户：只撤销这个租户，
// 不让它在合并后的运行时构建中拖垮其余租户。
func (snapshot publishedTraceSnapshot) withoutUnknownClusters(config map[string]any) publishedTraceSnapshot {
	known := make(map[string]struct{})
	if connections, ok := config["readonly_connections"].([]any); ok {
		for _, raw := range connections {
			if connection, ok := raw.(map[string]any); ok {
				known[fmt.Sprint(connection["cluster_id"])] = struct{}{}
			}
		}
	}
	revoked := make(map[string]string)
	for _, route := range snapshot.Routes {
		if _, ok := known[route.ClusterID]; !ok {
			if _, seen := revoked[route.BkTenantID]; !seen {
				revoked[route.BkTenantID] = route.ClusterID
			}
		}
	}
	if len(revoked) == 0 {
		return snapshot
	}
	result := snapshot
	result.Routes = make([]traceProfileRouteConfig, 0, len(snapshot.Routes))
	for _, route := range snapshot.Routes {
		if _, out := revoked[route.BkTenantID]; !out {
			result.Routes = append(result.Routes, route)
		}
	}
	result.Invalid = append([]invalidTraceDocument(nil), snapshot.Invalid...)
	tenants := make([]string, 0, len(revoked))
	for tenant := range revoked {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	for _, tenant := range tenants {
		document := snapshot.Documents[tenant]
		result.Invalid = append(result.Invalid, invalidTraceDocument{
			Key: document.Key, TenantID: tenant, Version: document.Version, Reason: traceDocumentUnknownCluster,
			Err: fmt.Errorf("Trace 读路由的集群缺少只读连接: %s", revoked[tenant]),
		})
	}
	return result
}

// traceProfileLoadState 记录读目标加载的连续失败与上一次结果，用于日志与指标。
type traceProfileLoadState struct {
	mu         sync.Mutex
	failures   int
	lastResult string
}

var traceProfileLoad traceProfileLoadState

func (s *traceProfileLoadState) consecutiveFailures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

func (s *traceProfileLoadState) record(result string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.lastResult
	s.lastResult = result
	switch result {
	case traceReloadSuccess, traceReloadUnchanged, traceReloadUnpublished, traceReloadRevoked:
		if s.failures > 0 {
			log.Infof(context.Background(), "trace profile read targets readable again after %d failures", s.failures)
		}
		s.failures = 0
		if result == traceReloadUnpublished && previous != traceReloadUnpublished {
			log.Warnf(context.Background(), "trace profile read targets: %s, direct read closed", errTraceReadTargetsUnpublished)
		}
	default:
		s.failures++
		if traceProfileRuntimeValue.Load() != nil {
			log.Warnf(context.Background(), "trace profile read targets %s (%d in a row), keeping the last good snapshot: %s",
				result, s.failures, err)
		} else {
			log.Errorf(context.Background(), "trace profile read targets %s (%d in a row), no snapshot installed: %s",
				result, s.failures, err)
		}
	}
	metric.TraceProfileSnapshotReload(traceProfileReadTargetSource(), result, s.failures)
}

// cachedTracePublication 是最近一次成功读取的发布快照与它来自的发布源。
type cachedTracePublication struct {
	source   string
	snapshot publishedTraceSnapshot
}

// traceProfileLoader 串行化所有改变直读运行时的路径（配置钩子、服务重载、监听重载），
// 并保存最近一次成功读取的发布快照：本地配置变化时直接用它重建，不必等发布源可读。
var traceProfileLoader struct {
	mu          sync.Mutex
	publication *cachedTracePublication
	// generation 每次配置钩子运行都加一；在它之前开始的读取结果作废。
	generation uint64
}

// traceProfileRuntimeNilReason 是直读运行时为 nil 的原因（traceRuntime*）。
var traceProfileRuntimeNilReason struct {
	mu     sync.Mutex
	reason string
}

func setTraceProfileRuntimeNilReason(reason string) {
	traceProfileRuntimeNilReason.mu.Lock()
	traceProfileRuntimeNilReason.reason = reason
	traceProfileRuntimeNilReason.mu.Unlock()
}

func traceProfileRuntimeClosedReason() string {
	traceProfileRuntimeNilReason.mu.Lock()
	defer traceProfileRuntimeNilReason.mu.Unlock()
	return traceProfileRuntimeNilReason.reason
}

// installTraceProfileRuntime 安装 runtime；runtime 为 nil 时 reason 说明为什么关闭直读。
func installTraceProfileRuntime(runtime *traceProfileRuntime, reason string) {
	if runtime == nil {
		setTraceProfileRuntimeNilReason(reason)
		metric.TraceProfileSnapshotConfirmed(time.Time{})
	} else {
		setTraceProfileRuntimeNilReason(traceRuntimeUnconfigured)
	}
	swapTraceProfileRuntime(runtime)
}

// traceProfileConfigDigest 覆盖构建运行时读到的全部本地输入：trace_profile 配置与多租户模式。
// 任何一项变化都要重建，授权相关的配置立即生效。
func traceProfileConfigDigest(encoded []byte) [sha256.Size]byte {
	hasher := sha256.New()
	_, _ = hasher.Write(encoded)
	if viper.GetBool(influxdb.MultiTenantModeConfigPath) {
		_, _ = hasher.Write([]byte{1})
	} else {
		_, _ = hasher.Write([]byte{0})
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

// traceProfileReadTimeout 限定一次发布源读取的总时长。读取在加载器锁之外进行，
// 超时只影响这一次重载，不会卡住配置钩子或信号处理。
const traceProfileReadTimeout = 10 * time.Second

// errTraceSourceMisconfigured 表示本地配置不足以读发布源（路径或键前缀为空），属于本地配置错误。
var errTraceSourceMisconfigured = errors.New("Trace 读目标发布源配置不完整")

// reloadTraceProfilePublishedRuntime 从配置的读目标发布源加载可信快照。
//
//   - 读取或传输失败：当前发布源已有可用快照时保留它，并计入连续失败；发布源刚切换、
//     还没有成功读取过时直读保持关闭；
//   - 读取成功但没有任何读目标：发布方明确未发布，直读关闭；
//   - 某个租户的文档非法，或它的路由指向本地没有只读连接的集群：只撤销该租户。
//
// 读取不持锁；安装时若配置钩子已经运行过（generation 变化），这次读取作废：它可能是
// 旧监听在配置切换后发起的，结果不能覆盖新配置下的状态。
func reloadTraceProfilePublishedRuntime() error {
	result, err, stale := loadTraceProfilePublishedRuntime()
	if stale {
		return nil
	}
	traceProfileLoad.record(result, err)
	return err
}

func loadTraceProfilePublishedRuntime() (string, error, bool) {
	traceProfileLoader.mu.Lock()
	generation := traceProfileLoader.generation
	published := traceProfileUsesPublishedRoutes()
	source := traceProfileSourceIdentity()
	traceProfileLoader.mu.Unlock()
	if !published {
		// 配置已不是发布源模式：这是切换后残留的监听，什么都不做。
		return "", nil, true
	}
	ctx, cancel := context.WithTimeout(context.Background(), traceProfileReadTimeout)
	defer cancel()
	pairs, unreadable, prefix, readErr := readPublishedTraceTargets(ctx)

	traceProfileLoader.mu.Lock()
	defer traceProfileLoader.mu.Unlock()
	if traceProfileLoader.generation != generation || traceProfileSourceIdentity() != source {
		return "", nil, true
	}
	if errors.Is(readErr, errTraceSourceMisconfigured) {
		traceProfileLoader.publication = nil
		installTraceProfileRuntime(nil, traceRuntimeInvalidConfig)
		return traceReloadInvalidConfig, readErr, false
	}
	if readErr != nil {
		if cached := traceProfileLoader.publication; cached == nil || cached.source != source {
			// 当前发布源从未成功读取过：不能用另一个发布源的快照服务。
			traceProfileLoader.publication = nil
			installTraceProfileRuntime(nil, traceRuntimeUnavailable)
		}
		return traceReloadReadFailed, readErr, false
	}
	snapshot, err := parsePublishedTraceTargets(prefix, pairs, unreadable)
	if errors.Is(err, errTraceReadTargetsUnpublished) {
		traceProfileLoader.publication = &cachedTracePublication{source: source}
		installTraceProfileRuntime(nil, traceRuntimeUnpublished)
		return traceReloadUnpublished, err, false
	}
	traceProfileLoader.publication = &cachedTracePublication{source: source, snapshot: snapshot}
	result, err := buildPublishedTraceRuntimeLocked()
	return result, err, false
}

// readPublishedTraceTargets 从配置的发布源读出全部发布项，读取受 ctx 限时。
func readPublishedTraceTargets(ctx context.Context) (api.KVPairs, map[string]error, string, error) {
	if traceProfileUsesRedis() {
		prefix, _, err := traceProfileRedisPaths()
		if err != nil {
			return nil, nil, "", fmt.Errorf("%w: %s", errTraceSourceMisconfigured, err)
		}
		pairs, unreadable, err := traceProfileRedisGetPairs(ctx, prefix)
		if err != nil {
			return nil, nil, "", fmt.Errorf("Trace 读目标文档读取失败: %w", err)
		}
		return pairs, unreadable, prefix, nil
	}
	dataPath := strings.TrimSuffix(viper.GetString(TraceProfileReadTargetDataPathConfigPath), "/")
	versionPath := viper.GetString(TraceProfileReadTargetVersionPathConfigPath)
	if dataPath == "" || versionPath == "" {
		return nil, nil, "", fmt.Errorf("%w: Consul 读目标路径或版本路径为空", errTraceSourceMisconfigured)
	}
	before, err := consul.GetKVDataContext(ctx, versionPath)
	if err != nil {
		return nil, nil, "", fmt.Errorf("Trace 读目标版本键不可用: %w", err)
	}
	prefix := dataPath + "/"
	pairs, err := consul.GetDataWithPrefixContext(ctx, prefix)
	if err != nil {
		return nil, nil, "", fmt.Errorf("Trace 读目标文档读取失败: %w", err)
	}
	after, err := consul.GetKVDataContext(ctx, versionPath)
	if err != nil {
		return nil, nil, "", fmt.Errorf("Trace 读目标版本键不可用: %w", err)
	}
	if !bytes.Equal(before, after) {
		return nil, nil, "", fmt.Errorf("Trace 读目标在加载期间发生变化")
	}
	if len(pairs) != 0 && len(before) == 0 {
		// 有发布项却没有版本键：无法确认读到的是一次完整发布，按读取失败处理。
		return nil, nil, "", fmt.Errorf("Trace 读目标版本键不存在")
	}
	return pairs, nil, prefix, nil
}

// buildPublishedTraceRuntimeLocked 用缓存的发布快照与当前本地配置构建运行时。配置钩子也走
// 这里，所以本地配置变化不依赖发布源可读。
func buildPublishedTraceRuntimeLocked() (string, error) {
	cached := traceProfileLoader.publication
	if cached == nil || cached.source != traceProfileSourceIdentity() {
		installTraceProfileRuntime(nil, traceRuntimeUnavailable)
		return traceReloadReadFailed, fmt.Errorf("Trace 读目标发布源尚未成功读取")
	}
	if cached.snapshot.Version == 0 {
		// 缓存的是一次"未发布"的读取。
		installTraceProfileRuntime(nil, traceRuntimeUnpublished)
		return traceReloadUnpublished, errTraceReadTargetsUnpublished
	}
	encoded, err := json.Marshal(viper.Get(TraceProfileConfigPath))
	if err != nil {
		installTraceProfileRuntime(nil, traceRuntimeInvalidConfig)
		return traceReloadInvalidConfig, fmt.Errorf("Trace 本地只读配置非法: %w", err)
	}
	configDigest := traceProfileConfigDigest(encoded)
	if current := traceProfileRuntimeValue.Load(); current != nil && current.published &&
		current.routeVersion == cached.snapshot.Version && current.configDigest == configDigest {
		metric.TraceProfileSnapshotConfirmed(time.Now())
		return traceReloadUnchanged, nil
	}
	var config map[string]any
	if err := json.Unmarshal(encoded, &config); err != nil || config == nil {
		installTraceProfileRuntime(nil, traceRuntimeInvalidConfig)
		return traceReloadInvalidConfig, fmt.Errorf("Trace 本地只读配置缺失或非法: %w", err)
	}
	if static, present := config["read_routes"]; present && static != nil {
		installTraceProfileRuntime(nil, traceRuntimeInvalidConfig)
		return traceReloadInvalidConfig, fmt.Errorf("Trace 已发布读目标不能与静态路由同时配置")
	}
	snapshot := cached.snapshot.withoutUnknownClusters(config)
	// 撤销只在重建时记录：快照与配置都不变时上面已经返回，不会每次重载都重复报同一份文档。
	var revokedErr error
	for _, invalid := range snapshot.Invalid {
		metric.TraceProfileInvalidDocument(invalid.Reason)
		log.Errorf(context.Background(),
			"trace profile read target of tenant %q (key %s, version %d) rejected (%s), tenant revoked: %s",
			invalid.TenantID, invalid.Key, invalid.Version, invalid.Reason, invalid.Err)
		revokedErr = errors.Join(revokedErr, fmt.Errorf("租户 %s 的 Trace 读目标被撤销: %w", invalid.TenantID, invalid.Err))
	}
	if len(snapshot.Routes) == 0 {
		// 读取成功，每个租户都被撤销：发布侧的结果，不是本地配置的问题。
		installTraceProfileRuntime(nil, traceRuntimeRevoked)
		return traceReloadRevoked, revokedErr
	}
	config["route_snapshot_version"] = snapshot.Version
	config["read_routes"] = snapshot.Routes
	runtime, err := newTraceProfileRuntime(config)
	if err != nil {
		// 本地配置与已校验的租户文档组合失败：不能确认任何授权，直读关闭。
		installTraceProfileRuntime(nil, traceRuntimeInvalidConfig)
		return traceReloadInvalidConfig, err
	}
	runtime.configDigest, runtime.published = configDigest, true
	installTraceProfileRuntime(runtime, "")
	metric.TraceProfileSnapshotConfirmed(time.Now())
	log.Infof(context.Background(), "trace profile published routes loaded: source=%s route-version=%d routes=%d revoked-tenants=%d",
		traceProfileReadTargetSource(), runtime.routeVersion, runtime.routeCount, len(snapshot.Invalid))
	return traceReloadSuccess, revokedErr
}

// applyTraceProfilePublishedConfig 由配置钩子在发布源模式下调用：本地配置变化立即生效。
// 发布源未变时用缓存的发布快照重建，不读发布源；发布源变化（或从未读取过）时直读关闭，
// 直到新发布源读取成功。
func applyTraceProfilePublishedConfig() {
	traceProfileLoader.mu.Lock()
	defer traceProfileLoader.mu.Unlock()
	traceProfileLoader.generation++
	cached := traceProfileLoader.publication
	if cached == nil || cached.source != traceProfileSourceIdentity() {
		traceProfileLoader.publication = nil
		installTraceProfileRuntime(nil, traceRuntimeUnavailable)
		return
	}
	result, err := buildPublishedTraceRuntimeLocked()
	if result == traceReloadInvalidConfig {
		traceProfileLoad.record(result, err)
	}
}

// applyTraceProfileStaticConfig 由配置钩子在静态模式下调用。
func applyTraceProfileStaticConfig() {
	traceProfileLoader.mu.Lock()
	defer traceProfileLoader.mu.Unlock()
	traceProfileLoader.generation++
	traceProfileLoader.publication = nil
	runtime, err := newTraceProfileRuntime(viper.Get(TraceProfileConfigPath))
	if err != nil {
		log.Errorf(context.Background(), "load trace profile direct read config failed: %s", err)
		installTraceProfileRuntime(nil, traceRuntimeInvalidConfig)
		return
	}
	installTraceProfileRuntime(runtime, traceRuntimeUnconfigured)
	if runtime == nil {
		return
	}
	log.Infof(
		context.Background(), "trace profile direct read enabled: route-version=%d routes=%d clusters=%d",
		runtime.routeVersion, runtime.routeCount, len(runtime.connections),
	)
}

func startTraceProfileRouteWatch(ctx context.Context, wg *sync.WaitGroup) error {
	versionPath := viper.GetString(TraceProfileReadTargetVersionPathConfigPath)
	watchChange := consul.WatchChangeContext
	if traceProfileUsesRedis() {
		_, channel, err := traceProfileRedisPaths()
		if err != nil {
			return err
		}
		versionPath = channel
		watchChange = traceProfileRedisWatchChangeContext
	} else if versionPath == "" {
		return fmt.Errorf("Trace 读目标版本路径未配置")
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDelay := time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			// 监听失败或关闭只影响变更通知的及时性，已安装的快照继续服务。
			ch, err := watchChange(ctx, versionPath)
			if err != nil {
				traceProfileLoad.record(traceReloadWatchFailed, fmt.Errorf("watch trace profile published routes failed: %w", err))
			} else {
				// A version change between the previous read and subscription must
				// be visible before this subscription starts serving requests.
				retryDelay = time.Second
				_ = reloadTraceProfilePublishedRuntime()
				ticker := time.NewTicker(time.Minute)
				watchOpen := true
				for watchOpen {
					select {
					case <-ctx.Done():
						ticker.Stop()
						return
					case _, open := <-ch:
						if !open {
							traceProfileLoad.record(traceReloadWatchFailed, fmt.Errorf("trace profile published routes watch closed"))
							watchOpen = false
						}
					case <-ticker.C:
					}
					if watchOpen {
						_ = reloadTraceProfilePublishedRuntime()
					}
				}
				ticker.Stop()
			}
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if retryDelay < 30*time.Second {
				retryDelay *= 2
				if retryDelay > 30*time.Second {
					retryDelay = 30 * time.Second
				}
			}
		}
	}()
	return nil
}
