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
	traceReloadSuccess       = "success"
	traceReloadUnchanged     = "unchanged"
	traceReloadUnpublished   = "unpublished"
	traceReloadReadFailed    = "read_failed"
	traceReloadWatchFailed   = "watch_failed"
	traceReloadInvalidConfig = "invalid_config"
)

// 单个租户文档被拒的原因，作为指标标签的固定枚举。
const (
	traceDocumentInvalidPath      = "invalid_path"
	traceDocumentDuplicateTenant  = "duplicate_tenant"
	traceDocumentInvalidDocument  = "invalid_document"
	traceDocumentEmptyPublication = "empty_value"
)

// errTraceReadTargetsUnpublished 表示读取成功但没有任何读目标：这是发布方的明确状态，
// 与读取失败不同，直读随之关闭。
var errTraceReadTargetsUnpublished = errors.New("Trace 读目标尚未发布")

// traceProfileReadTargetSource 是 Trace 读目标的发布源，未配置时为 consul：
// 当前发布方只写 Consul，切到 redis 必须显式配置，且应在发布方写入 Redis 之后。
func traceProfileReadTargetSource() string {
	source := viper.GetString(TraceProfileReadTargetSourceConfigPath)
	if source == "" {
		return traceReadTargetSourceConsul
	}
	return source
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

// invalidTraceDocument 是一份未通过校验的租户文档：该租户被撤销，其余租户照常加载。
type invalidTraceDocument struct {
	Key      string
	TenantID string
	Version  int64
	Reason   string
	Err      error
}

// publishedTraceSnapshot 是一次成功读取得到的读目标快照。
type publishedTraceSnapshot struct {
	Version   int64
	Routes    []traceProfileRouteConfig
	ESTargets []traceProfileESTargetConfig
	Invalid   []invalidTraceDocument
}

// parsePublishedTraceTargets 逐个租户校验发布文档。非法文档只撤销它自己的租户：
// 这些文档携带授权（租户路由与成员目标），收紧后的文档校验失败时不能保留旧的、更宽的授权，
// 也不能让一个租户的坏文档拖垮其余租户。快照版本覆盖每个键的完整内容，
// 内容变化而文档版本未变时同样会重建。
func parsePublishedTraceTargets(prefix string, pairs api.KVPairs) (publishedTraceSnapshot, error) {
	if !strings.HasSuffix(prefix, "/") && !strings.HasSuffix(prefix, ":") {
		prefix += "/"
	}
	if len(pairs) == 0 {
		return publishedTraceSnapshot{}, errTraceReadTargetsUnpublished
	}
	sorted := make(api.KVPairs, 0, len(pairs))
	for _, pair := range pairs {
		if pair != nil {
			sorted = append(sorted, pair)
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	snapshot := publishedTraceSnapshot{
		Routes:    make([]traceProfileRouteConfig, 0),
		ESTargets: make([]traceProfileESTargetConfig, 0),
	}
	hasher := sha256.New()
	byTenant := make(map[string][]int, len(sorted))
	documents := make([]traceprofile.PublishedDocument, len(sorted))
	for index, pair := range sorted {
		content := sha256.Sum256(pair.Value)
		_, _ = hasher.Write([]byte(pair.Key + "\x00"))
		_, _ = hasher.Write(content[:])
		tenantID := strings.TrimPrefix(pair.Key, prefix)
		invalid := invalidTraceDocument{Key: pair.Key, TenantID: tenantID, Version: publishedDocumentVersion(pair.Value)}
		switch {
		case !strings.HasPrefix(pair.Key, prefix) || tenantID == "" || strings.Contains(tenantID, "/"):
			invalid.Reason, invalid.Err = traceDocumentInvalidPath, fmt.Errorf("Trace 读目标路径缺少单一租户")
		case len(pair.Value) == 0:
			invalid.Reason, invalid.Err = traceDocumentEmptyPublication, fmt.Errorf("Trace 读目标发布项为空")
		default:
			document, err := traceprofile.ParsePublishedDocument(pair.Value, tenantID)
			if err != nil {
				invalid.Reason, invalid.Err = traceDocumentInvalidDocument, err
			} else {
				documents[index] = document
				byTenant[tenantID] = append(byTenant[tenantID], index)
				continue
			}
		}
		snapshot.Invalid = append(snapshot.Invalid, invalid)
	}
	for _, pair := range sorted {
		tenantID := strings.TrimPrefix(pair.Key, prefix)
		indexes := byTenant[tenantID]
		if len(indexes) == 0 {
			continue
		}
		delete(byTenant, tenantID)
		if len(indexes) > 1 {
			// 同一租户出现多份文档时无法判断哪份是发布方的意图，整个租户撤销。
			for _, index := range indexes {
				snapshot.Invalid = append(snapshot.Invalid, invalidTraceDocument{
					Key: sorted[index].Key, TenantID: tenantID, Version: documents[index].Version,
					Reason: traceDocumentDuplicateTenant, Err: fmt.Errorf("Trace 读目标重复发布租户 %s", tenantID),
				})
			}
			continue
		}
		document := documents[indexes[0]]
		for _, route := range document.Routes {
			snapshot.Routes = append(snapshot.Routes, traceProfileRouteConfig{
				BkTenantID: route.BkTenantID, BkBizID: route.BkBizID,
				Profile: string(route.Profile), ObservationDomainID: route.ObservationDomainID,
				Database: route.Doris.Database, Table: route.Doris.Table,
				ClusterID: route.Doris.ClusterID, SchemaVersion: route.Doris.SchemaVersion,
				PublishedVersion: document.Version,
			})
		}
		for _, target := range document.ESSummaryTargets {
			snapshot.ESTargets = append(snapshot.ESTargets, traceProfileESTargetConfig{
				BkTenantID: target.BkTenantID, BkBizID: target.BkBizID,
				AppID: target.AppID, AppName: target.AppName,
				ObservationDomainID: target.ObservationDomainID,
				ResultTableID:       target.ResultTableID,
				StorageClusterID:    target.StorageClusterID,
				RetentionDays:       target.RetentionDays,
				PublishedVersion:    document.Version,
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

// traceProfileLoadState 记录读目标加载的连续失败，用于日志与指标。
type traceProfileLoadState struct {
	mu       sync.Mutex
	failures int
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
	switch result {
	case traceReloadSuccess, traceReloadUnchanged, traceReloadUnpublished:
		if s.failures > 0 {
			log.Infof(context.Background(), "trace profile read targets readable again after %d failures", s.failures)
		}
		s.failures = 0
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

// traceProfileConfigDigest 覆盖本地 trace_profile 配置；配置热更新后即使发布版本不变也要重建。
func traceProfileConfigDigest(encoded []byte) [sha256.Size]byte {
	return sha256.Sum256(encoded)
}

// reloadTraceProfilePublishedRuntime 从配置的读目标发布源加载可信快照。
//
//   - 读取或传输失败：保留上一份可用快照，并计入连续失败；
//   - 读取成功但没有任何读目标：发布方明确未发布，直读关闭；
//   - 某个租户的文档非法：只撤销该租户，其余租户照常加载。
func reloadTraceProfilePublishedRuntime() error {
	result, err := loadTraceProfilePublishedRuntime()
	traceProfileLoad.record(result, err)
	return err
}

func loadTraceProfilePublishedRuntime() (string, error) {
	var prefix string
	var pairs api.KVPairs
	var err error
	if traceProfileUsesRedis() {
		prefix, _, err = traceProfileRedisPaths()
		if err != nil {
			return traceReloadReadFailed, err
		}
		pairs, err = traceProfileRedisGetPairs(prefix)
		if err != nil {
			return traceReloadReadFailed, fmt.Errorf("Trace 读目标文档读取失败: %w", err)
		}
	} else {
		dataPath := strings.TrimSuffix(viper.GetString(TraceProfileReadTargetDataPathConfigPath), "/")
		versionPath := viper.GetString(TraceProfileReadTargetVersionPathConfigPath)
		if dataPath == "" || versionPath == "" {
			return traceReloadReadFailed, fmt.Errorf("Trace Consul 读目标路径或版本路径未配置")
		}
		before, readErr := consul.GetKVData(versionPath)
		if readErr != nil {
			return traceReloadReadFailed, fmt.Errorf("Trace 读目标版本键不可用: %w", readErr)
		}
		prefix = dataPath + "/"
		pairs, err = consul.GetDataWithPrefix(prefix)
		if err != nil {
			return traceReloadReadFailed, fmt.Errorf("Trace 读目标文档读取失败: %w", err)
		}
		after, readErr := consul.GetKVData(versionPath)
		if readErr != nil {
			return traceReloadReadFailed, fmt.Errorf("Trace 读目标版本键不可用: %w", readErr)
		}
		if !bytes.Equal(before, after) {
			return traceReloadReadFailed, fmt.Errorf("Trace 读目标在加载期间发生变化")
		}
		if len(pairs) != 0 && len(before) == 0 {
			return traceReloadReadFailed, fmt.Errorf("Trace 读目标版本键不存在")
		}
	}
	snapshot, err := parsePublishedTraceTargets(prefix, pairs)
	if errors.Is(err, errTraceReadTargetsUnpublished) {
		revokeTraceProfileRuntime()
		return traceReloadUnpublished, err
	}
	if err != nil {
		return traceReloadReadFailed, err
	}
	var invalidErr error
	for _, invalid := range snapshot.Invalid {
		metric.TraceProfileInvalidDocument(invalid.Reason)
		log.Errorf(context.Background(),
			"trace profile read target of tenant %q (key %s, version %d) rejected (%s), tenant revoked: %s",
			invalid.TenantID, invalid.Key, invalid.Version, invalid.Reason, invalid.Err)
		invalidErr = errors.Join(invalidErr, fmt.Errorf("租户 %s 的 Trace 读目标非法: %w", invalid.TenantID, invalid.Err))
	}
	encoded, err := json.Marshal(viper.Get(TraceProfileConfigPath))
	if err != nil {
		revokeTraceProfileRuntime()
		return traceReloadInvalidConfig, fmt.Errorf("Trace 本地只读配置非法: %w", err)
	}
	configDigest := traceProfileConfigDigest(encoded)
	if current := traceProfileRuntimeValue.Load(); current != nil &&
		current.routeVersion == snapshot.Version && current.configDigest == configDigest {
		metric.TraceProfileSnapshotConfirmed(time.Now())
		return traceReloadUnchanged, invalidErr
	}
	var config map[string]any
	if err := json.Unmarshal(encoded, &config); err != nil || config == nil {
		revokeTraceProfileRuntime()
		return traceReloadInvalidConfig, fmt.Errorf("Trace 本地只读配置缺失或非法: %w", err)
	}
	if static, present := config["read_routes"]; present && static != nil {
		revokeTraceProfileRuntime()
		return traceReloadInvalidConfig, fmt.Errorf("Trace 已发布读目标不能与静态路由同时配置")
	}
	config["route_snapshot_version"] = snapshot.Version
	config["read_routes"] = snapshot.Routes
	config["es_summary_targets"] = snapshot.ESTargets
	runtime, err := newTraceProfileRuntime(config)
	if err != nil {
		// 本地配置与已校验的租户文档组合失败：不能确认任何授权，直读关闭。
		revokeTraceProfileRuntime()
		return traceReloadInvalidConfig, err
	}
	if runtime == nil {
		// 读取成功，但所有租户都被撤销或没有发布任何目标。
		revokeTraceProfileRuntime()
		if invalidErr != nil {
			return traceReloadSuccess, invalidErr
		}
		return traceReloadUnpublished, errTraceReadTargetsUnpublished
	}
	runtime.configDigest = configDigest
	swapTraceProfileRuntime(runtime)
	metric.TraceProfileSnapshotConfirmed(time.Now())
	log.Infof(context.Background(), "trace profile published routes loaded: source=%s route-version=%d routes=%d revoked-tenants=%d",
		traceProfileReadTargetSource(), runtime.routeVersion, runtime.routeCount, len(snapshot.Invalid))
	return traceReloadSuccess, invalidErr
}

func revokeTraceProfileRuntime() {
	swapTraceProfileRuntime(nil)
	metric.TraceProfileSnapshotConfirmed(time.Time{})
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
