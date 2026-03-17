#!/bin/bash

# Datadog Receiver 测试脚本
# 用于向 Datadog receiver 发送测试 RUM 数据

HOST=${1:-"localhost"}
PORT=${2:-"4319"}
TOKEN=${3:-"test-token"}

echo "===== Datadog Receiver 测试脚本 ====="
echo "目标地址: $HOST:$PORT"
echo "认证 Token: $TOKEN"
echo ""

# =============================================
# 测试 1: 发送单个 RUM 事件 (V1 格式)
# =============================================
echo "测试 1: 发送单个 RUM 事件 (V1 格式)"
echo "---"

RUM_EVENT_V1='{
  "type": "view",
  "data": {
    "message": "Page loaded",
    "level": "info",
    "session": {
      "id": "session-123"
    },
    "view": {
      "name": "login",
      "url": "https://example.com/login"
    }
  },
  "meta": {
    "user_agent": "Mozilla/5.0...",
    "remote_ip": "192.168.1.1"
  }
}'

curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-BK-TOKEN: $TOKEN" \
  -d "$RUM_EVENT_V1" \
  http://$HOST:$PORT/api/v2/rum

echo ""
echo "✓ 测试 1 完成"
echo ""

# =============================================
# 测试 2: 发送多个 RUM 事件 (V1 格式 - 数组)
# =============================================
echo "测试 2: 发送多个 RUM 事件 (V1 格式 - 数组)"
echo "---"

RUM_EVENTS_V1_ARRAY='[
  {
    "type": "view",
    "data": {
      "message": "Page loaded",
      "level": "info",
      "session": {"id": "session-123"},
      "view": {"name": "home", "url": "https://example.com"}
    }
  },
  {
    "type": "action",
    "data": {
      "message": "User clicked button",
      "level": "info",
      "session": {"id": "session-123"},
      "action": {"type": "click", "target": "button.submit"}
    }
  }
]'

curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-BK-TOKEN: $TOKEN" \
  -d "$RUM_EVENTS_V1_ARRAY" \
  http://$HOST:$PORT/api/v2/rum

echo ""
echo "✓ 测试 2 完成"
echo ""

# =============================================
# 测试 3: 发送 RUM 事件 (V2 格式 - JSON Lines)
# =============================================
echo "测试 3: 发送 RUM 事件 (V2 格式 - JSON Lines)"
echo "---"

RUM_EVENTS_V2_JSONLINES=$(cat <<'EOF'
{"type":"view","event_type":"page_view","timestamp":1234567890000,"data":{"message":"Page loaded","level":"info"},"meta":{"user_agent":"Mozilla/5.0"},"context":{"source":"browser","version":"1.0.0"}}
{"type":"action","event_type":"click","timestamp":1234567891000,"data":{"message":"User clicked button","level":"info"},"meta":{"user_agent":"Mozilla/5.0"},"context":{"source":"browser","version":"1.0.0"}}
{"type":"error","event_type":"javascript_error","timestamp":1234567892000,"data":{"message":"Uncaught SyntaxError","level":"error"},"meta":{"user_agent":"Mozilla/5.0"},"context":{"source":"browser","version":"1.0.0"}}
EOF
)

echo "$RUM_EVENTS_V2_JSONLINES" | curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-BK-TOKEN: $TOKEN" \
  --data-binary @- \
  http://$HOST:$PORT/api/v2/rum/events

echo ""
echo "✓ 测试 3 完成"
echo ""

# =============================================
# 测试 4: 发送 RUM 事件 (V2 格式 - 数组)
# =============================================
echo "测试 4: 发送 RUM 事件 (V2 格式 - 数组)"
echo "---"

RUM_EVENTS_V2_ARRAY='[
  {
    "type": "view",
    "event_type": "page_view",
    "timestamp": 1234567890000,
    "data": {
      "message": "Dashboard loaded",
      "level": "info"
    },
    "meta": {
      "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "remote_ip": "192.168.1.100"
    },
    "context": {
      "source": "browser",
      "version": "1.0.0"
    }
  },
  {
    "type": "performance",
    "event_type": "resource_timing",
    "timestamp": 1234567891000,
    "data": {
      "message": "API request completed",
      "level": "info",
      "resource": {
        "name": "https://api.example.com/data",
        "duration": 150,
        "size": 1024
      }
    },
    "meta": {
      "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    },
    "context": {
      "source": "browser",
      "version": "1.0.0"
    }
  }
]'

curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-BK-TOKEN: $TOKEN" \
  -d "$RUM_EVENTS_V2_ARRAY" \
  http://$HOST:$PORT/api/v2/rum/events

echo ""
echo "✓ 测试 4 完成"
echo ""

echo "===== 所有测试完成 ====="
echo ""
echo "验证接收器日志以确认数据已正确接收和处理"
