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
	"strings"
	"time"

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

func traceProfileRedisGetPairs(prefix string) (api.KVPairs, error) {
	client := redis.Client()
	if client == nil {
		return nil, fmt.Errorf("Trace Redis 客户端未初始化")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// 每个租户一条 String key；SCAN 可能重复返回 key，读取后由原解析器校验完整文档。
	pairs := make(api.KVPairs, 0)
	seen := make(map[string]struct{})
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if _, duplicated := seen[key]; duplicated {
				continue
			}
			seen[key] = struct{}{}
			value, err := client.Get(ctx, key).Bytes()
			if err != nil {
				return nil, fmt.Errorf("Trace Redis 读目标 %s 读取失败: %w", key, err)
			}
			pairs = append(pairs, &api.KVPair{Key: key, Value: value})
		}
		cursor = next
		if cursor == 0 {
			return pairs, nil
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
