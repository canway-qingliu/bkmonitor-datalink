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
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	goRedis "github.com/go-redis/redis/v8"
	"github.com/hashicorp/consul/api"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/redis"
	redisService "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/service/redis"
)

func traceProfileRedisPaths() (string, string, error) {
	basePath := strings.TrimSuffix(redisService.KVBasePath, ":")
	if basePath == "" {
		return "", "", fmt.Errorf("Trace Redis 键前缀未配置")
	}
	prefix := basePath + ":data:trace:read_target:"
	return prefix, prefix + "read_target_channel", nil
}

// traceProfileRedisGetPairs 读出前缀下的全部发布项。unreadable 是存在、但不是字符串的键，
// 由调用方按该键的非法文档处理。
func traceProfileRedisGetPairs(prefix string) (api.KVPairs, map[string]error, error) {
	client := redis.Client()
	if client == nil {
		return nil, nil, fmt.Errorf("Trace Redis 客户端未初始化")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// 每个租户一条 String key。SCAN 不是快照，所以：先列出键，再用一次 MGET 原子读取
	// 所有值，最后重扫一次键集合。
	//   - MGET 为 nil、重扫也没有的键：扫描后被删除，按已撤销跳过；
	//   - MGET 为 nil、重扫仍有的键：不是字符串（TYPE 确认），按该键的非法文档处理；
	//   - 两次键集合有其它差异：加载期间有发布，本次按读取失败处理，保留上一份快照。
	keys, err := traceProfileRedisScanKeys(ctx, client, prefix)
	if err != nil {
		return nil, nil, err
	}
	pairs := make(api.KVPairs, 0, len(keys))
	missing := make(map[string]struct{})
	if len(keys) > 0 {
		values, err := client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, nil, fmt.Errorf("Trace Redis 读目标读取失败: %w", err)
		}
		for index, value := range values {
			text, ok := value.(string)
			if !ok {
				missing[keys[index]] = struct{}{}
				continue
			}
			pairs = append(pairs, &api.KVPair{Key: keys[index], Value: []byte(text)})
		}
	}
	after, err := traceProfileRedisScanKeys(ctx, client, prefix)
	if err != nil {
		return nil, nil, err
	}
	read := make(map[string]struct{}, len(pairs))
	for _, pair := range pairs {
		read[pair.Key] = struct{}{}
	}
	changed := fmt.Errorf("Trace Redis 读目标在加载期间发生变化")
	unreadable := make(map[string]error)
	seen := 0
	for _, key := range after {
		if _, ok := read[key]; ok {
			seen++
			continue
		}
		if _, ok := missing[key]; !ok {
			return nil, nil, changed
		}
		kind, err := client.Type(ctx, key).Result()
		if err != nil {
			return nil, nil, fmt.Errorf("Trace Redis 读目标读取失败: %w", err)
		}
		if kind == "string" || kind == "none" {
			// MGET 之后被重建或删除：加载期间有发布。
			return nil, nil, changed
		}
		unreadable[key] = fmt.Errorf("Trace Redis 读目标不是字符串（%s）", kind)
	}
	if seen != len(pairs) {
		return nil, nil, changed
	}
	return pairs, unreadable, nil
}

// traceProfileRedisScanKeys 列出前缀下全部键，去重并排序；SCAN 可能重复返回同一个键。
func traceProfileRedisScanKeys(ctx context.Context, client goRedis.UniversalClient, prefix string) ([]string, error) {
	seen := make(map[string]struct{})
	keys := make([]string, 0)
	var cursor uint64
	for {
		batch, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range batch {
			if _, duplicated := seen[key]; duplicated {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		cursor = next
		if cursor == 0 {
			sort.Strings(keys)
			return keys, nil
		}
	}
}

func traceProfileRedisWatchChangeContext(ctx context.Context, path string) (<-chan any, error) {
	client := redis.Client()
	if client == nil {
		return nil, fmt.Errorf("Trace Redis 客户端未初始化")
	}
	pubSub := client.Subscribe(ctx, path)
	stopClose := context.AfterFunc(ctx, func() { _ = pubSub.Close() })
	defer stopClose()
	if _, err := pubSub.ReceiveTimeout(ctx, 5*time.Second); err != nil {
		_ = pubSub.Close()
		return nil, fmt.Errorf("Trace Redis 读目标订阅失败: %w", err)
	}
	result := make(chan any, 1)
	go func() {
		defer pubSub.Close()
		defer close(result)
		messages := pubSub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-messages:
				if !ok {
					return
				}
				select {
				case result <- struct{}{}:
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}()
	return result, nil
}
