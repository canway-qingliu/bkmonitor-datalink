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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/traceprofile"
)

func traceProfileUsesConsul() bool {
	return viper.GetString(TraceProfileReadTargetDataPathConfigPath) != ""
}

// parsePublishedTraceTargets 只接受 Kingeye 发布的单租户文档；快照版本由各
// 租户文档的版本确定，任何缺失或错误文档都不能留下上一轮的物理目标。
func parsePublishedTraceTargets(prefix string, pairs api.KVPairs) (int64, []traceProfileRouteConfig, []traceProfileESTargetConfig, error) {
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	if len(pairs) == 0 {
		return 0, nil, nil, fmt.Errorf("Trace 读目标尚未发布")
	}
	sorted := append(api.KVPairs(nil), pairs...)
	for _, pair := range sorted {
		if pair == nil {
			return 0, nil, nil, fmt.Errorf("Trace 读目标 Consul 项为空")
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	hasher := sha256.New()
	routes := make([]traceProfileRouteConfig, 0)
	esTargets := make([]traceProfileESTargetConfig, 0)
	seen := make(map[string]struct{}, len(sorted))
	for _, pair := range sorted {
		if pair == nil || !strings.HasPrefix(pair.Key, prefix) || len(pair.Value) == 0 {
			return 0, nil, nil, fmt.Errorf("Trace 读目标 Consul 项缺失或路径非法")
		}
		tenantID := strings.TrimPrefix(pair.Key, prefix)
		if tenantID == "" || strings.Contains(tenantID, "/") {
			return 0, nil, nil, fmt.Errorf("Trace 读目标路径缺少单一租户")
		}
		if _, duplicated := seen[tenantID]; duplicated {
			return 0, nil, nil, fmt.Errorf("Trace 读目标重复发布租户 %s", tenantID)
		}
		seen[tenantID] = struct{}{}
		document, err := traceprofile.ParsePublishedDocument(pair.Value, tenantID)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("租户 %s 的 Trace 读目标非法: %w", tenantID, err)
		}
		_, _ = hasher.Write([]byte(tenantID + "\x00" + strconv.FormatInt(document.Version, 10) + "\x00"))
		for _, route := range document.Routes {
			routes = append(routes, traceProfileRouteConfig{
				BkTenantID: route.BkTenantID, BkBizID: route.BkBizID,
				Profile: string(route.Profile), ObservationDomainID: route.ObservationDomainID,
				Database: route.Doris.Database, Table: route.Doris.Table,
				ClusterID: route.Doris.ClusterID, SchemaVersion: route.Doris.SchemaVersion,
				PublishedVersion: document.Version,
			})
		}
		for _, target := range document.ESSummaryTargets {
			esTargets = append(esTargets, traceProfileESTargetConfig{
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
	version := int64(binary.BigEndian.Uint64(digest[:8]) & uint64(^uint64(0)>>1))
	if version == 0 {
		version = 1
	}
	return version, routes, esTargets, nil
}

// reloadTraceProfilePublishedRuntime 从版本键触发的可信数据前缀加载完整快照。
// 失败时撤销当前直读路由，避免使用可能已被撤销的旧物理目标。
func reloadTraceProfilePublishedRuntime() (err error) {
	defer func() {
		if err != nil {
			swapTraceProfileRuntime(nil)
		}
	}()
	dataPath := strings.TrimSuffix(viper.GetString(TraceProfileReadTargetDataPathConfigPath), "/")
	versionPath := viper.GetString(TraceProfileReadTargetVersionPathConfigPath)
	if dataPath == "" || versionPath == "" {
		return fmt.Errorf("Trace Consul 读目标路径或版本路径未配置")
	}
	before, err := consul.GetKVData(versionPath)
	if err != nil {
		return fmt.Errorf("Trace 读目标版本键不可用: %w", err)
	}
	if len(before) == 0 {
		return fmt.Errorf("Trace 读目标版本键不存在")
	}
	pairs, err := consul.GetDataWithPrefix(dataPath + "/")
	if err != nil {
		return fmt.Errorf("Trace 读目标文档读取失败: %w", err)
	}
	after, err := consul.GetKVData(versionPath)
	if err != nil {
		return fmt.Errorf("Trace 读目标在加载期间发生变化: %w", err)
	}
	if !bytes.Equal(before, after) {
		return fmt.Errorf("Trace 读目标在加载期间发生变化")
	}
	version, routes, esTargets, err := parsePublishedTraceTargets(dataPath+"/", pairs)
	if err != nil {
		return err
	}
	if current := traceProfileRuntimeValue.Load(); current != nil && current.routeVersion == version {
		return nil
	}
	encoded, err := json.Marshal(viper.Get(TraceProfileConfigPath))
	if err != nil {
		return fmt.Errorf("Trace 本地只读配置非法: %w", err)
	}
	var config map[string]any
	if err := json.Unmarshal(encoded, &config); err != nil || config == nil {
		return fmt.Errorf("Trace 本地只读配置缺失或非法: %w", err)
	}
	if static, present := config["read_routes"]; present && static != nil {
		return fmt.Errorf("Trace Consul 读目标不能与静态路由同时配置")
	}
	config["route_snapshot_version"] = version
	config["read_routes"] = routes
	config["es_summary_targets"] = esTargets
	runtime, err := newTraceProfileRuntime(config)
	if err != nil {
		return err
	}
	if runtime == nil {
		return fmt.Errorf("Trace 已发布读目标没有可用路由")
	}
	swapTraceProfileRuntime(runtime)
	log.Infof(context.Background(), "trace profile published routes loaded: route-version=%d routes=%d", runtime.routeVersion, runtime.routeCount)
	return nil
}

func startTraceProfileRouteWatch(ctx context.Context, wg *sync.WaitGroup) error {
	versionPath := viper.GetString(TraceProfileReadTargetVersionPathConfigPath)
	if versionPath == "" {
		swapTraceProfileRuntime(nil)
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
			ch, err := consul.WatchChangeContext(ctx, versionPath)
			if err != nil {
				swapTraceProfileRuntime(nil)
				log.Errorf(ctx, "watch trace profile published routes failed: %s", err)
			} else {
				// A version change between the previous read and subscription must
				// be visible before this subscription starts serving requests.
				retryDelay = time.Second
				if err := reloadTraceProfilePublishedRuntime(); err != nil {
					log.Errorf(ctx, "reload trace profile published routes failed: %s", err)
				}
				ticker := time.NewTicker(time.Minute)
				watchOpen := true
				for watchOpen {
					select {
					case <-ctx.Done():
						ticker.Stop()
						return
					case _, open := <-ch:
						if !open {
							swapTraceProfileRuntime(nil)
							watchOpen = false
						}
					case <-ticker.C:
					}
					if watchOpen {
						if err := reloadTraceProfilePublishedRuntime(); err != nil {
							log.Errorf(ctx, "reload trace profile published routes failed: %s", err)
						}
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
