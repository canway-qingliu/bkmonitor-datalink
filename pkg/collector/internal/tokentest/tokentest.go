// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Package tokentest checks, for the tests of the packages that write
// self-monitoring metrics, that no series names an application's token: it
// imports nothing of the collector, so any of them can use it.
package tokentest

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Fingerprint is a token's fingerprint as the metrics are to name it, worked
// out here rather than taken from the code under test: the hex of the first
// eight bytes of its SHA-256.
func Fingerprint(original string) string {
	digest := sha256.Sum256([]byte(original))
	return hex.EncodeToString(digest[:8])
}

// CheckFamilies reads the families named from the default gatherer after a
// test has recorded under original. Each must hold a series whose
// token_fingerprint label is original's fingerprint, and no series of any of
// them may have a label named token or a label value equal to original.
func CheckFamilies(t testing.TB, original string, names ...string) {
	t.Helper()
	want := Fingerprint(original)
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, family := range families {
		name := family.GetName()
		if !slices.Contains(names, name) {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "token" {
					t.Errorf("%s has a label named token", name)
				}
				if label.GetValue() == original {
					t.Errorf("%s carries the raw token as its %s label", name, label.GetName())
				}
				if label.GetName() == "token_fingerprint" && label.GetValue() == want {
					found[name] = true
				}
			}
		}
	}
	for _, name := range names {
		if !found[name] {
			t.Errorf("%s has no series under the token's fingerprint %s", name, want)
		}
	}
}
