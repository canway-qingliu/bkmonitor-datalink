// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package trace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstanceIdentityValue(t *testing.T) {
	tests := []struct {
		name   string
		source instanceIdentity
		want   string
	}{
		{
			name: "kubernetes without a deployment-given id uses ip",
			source: instanceIdentity{
				hostname: "abcdef123456", ip: "10.0.28.136", inKubernetes: true, inDocker: true,
			},
			want: "unify-query@10.0.28.136",
		},
		{
			name: "docker explicit container id",
			source: instanceIdentity{
				containerID: "abcdef1234567890", hostname: "custom-uq", ip: "172.17.0.2", inDocker: true,
			},
			want: "docker_abcdef1234567890",
		},
		{
			name: "docker default hostname is short container id",
			source: instanceIdentity{
				hostname: "abcdef123456", ip: "172.17.0.2", inDocker: true,
			},
			want: "docker_abcdef123456",
		},
		{
			name:   "host uses ip",
			source: instanceIdentity{hostname: "linux-host", ip: "10.10.28.10"},
			want:   "unify-query@10.10.28.10",
		},
		{
			name:   "invalid docker override does not change host identity",
			source: instanceIdentity{containerID: "custom", hostname: "linux-host", ip: "10.10.28.10"},
			want:   "unify-query@10.10.28.10",
		},
		{
			name:   "host without ipv4 uses hostname",
			source: instanceIdentity{hostname: "linux-host"},
			want:   "unify-query@linux-host",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.source.value("unify-query"))
		})
	}
}

func TestDockerCustomHostnameRequiresContainerID(t *testing.T) {
	source := instanceIdentity{hostname: "custom-uq", ip: "172.17.0.2", inDocker: true}
	require.Empty(t, source.value("unify-query"))
}

func TestNewResourceUsesDockerContainerIDOverride(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("UQ_CONTAINER_ID", "abcdef1234567890")

	var instanceID string
	for _, item := range new(Service).newResource().Attributes() {
		if item.Key == "service.instance.id" {
			instanceID = item.Value.AsString()
		}
	}
	require.Equal(t, "docker_abcdef1234567890", instanceID)
}

func resourceInstanceID(t *testing.T) (string, bool) {
	t.Helper()
	for _, item := range new(Service).newResource().Attributes() {
		if item.Key == serviceInstanceIDKey {
			return item.Value.AsString(), true
		}
	}
	return "", false
}

// The deployment's service.instance.id decides: the code resource leaves the
// key out so that the environment resource WithResource merges under it
// keeps it.
func TestNewResourceLeavesTheDeploymentGivenInstanceID(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=kingeye.unify-query-0.unify-query,k8s.pod.name=unify-query-0")
	_, carried := resourceInstanceID(t)
	require.False(t, carried, "a deployment-given service.instance.id must not be overridden by the code resource")
}

// Without one, or with an empty one, the code still names the instance.
func TestNewResourceFallsBackWithoutADeploymentGivenInstanceID(t *testing.T) {
	for _, env := range []string{"k8s.pod.name=unify-query-0", "service.instance.id=,k8s.pod.name=unify-query-0"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", env)
			instanceID, carried := resourceInstanceID(t)
			require.True(t, carried)
			require.Contains(t, instanceID, ServiceName+"@")
		})
	}
}
