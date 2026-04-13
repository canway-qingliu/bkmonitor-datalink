# syntax=docker/dockerfile:1

# ==========================================
# 第一阶段：构建 (Builder)
# ==========================================
FROM golang:1.23-alpine AS builder

# 安装必要的编译工具 (git 用于拉取私有依赖，gcc 仅在需要 CGO 时安装)
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# 1. 优先复制依赖文件，利用 Docker 缓存层
# 只有当 go.mod 或 go.sum 变动时，后续的 RUN 才会重新执行
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# 2. 复制源代码
COPY . .

# 3. 编译优化
# CGO_ENABLED=0: 生成静态二进制，不依赖 libc (关键！)
# -ldflags="-s -w": 去除符号表和调试信息，减小体积
# -o /app/server: 指定输出路径
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /app/server .

# ==========================================
# 第二阶段：运行 (Runtime)
# ==========================================
# 方案 A: 极致轻量 (推荐，体积 < 20MB)
# 使用 alpine 作为基础镜像，包含基本的证书和时区，比 scratch 更稳健
FROM alpine:3.19 AS runner

# 安装 ca-certificates 以支持 HTTPS 请求
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# 从构建阶段复制编译好的二进制文件
COPY --from=builder /app/server .

# 暴露端口 (仅作为文档说明，不实际发布端口)
EXPOSE 8080

# 启动命令
CMD ["./server"]

# ==========================================
# 备选方案 B: 绝对最小化 (Scratch)
# 如果你的程序完全静态且不需要访问外部 HTTPS，可用 scratch
# FROM scratch
# COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# COPY --from=builder /app/server .
# CMD ["./server"]