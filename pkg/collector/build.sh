GOOS=linux GOARCH=amd64 \
        go build -tags "jsonsonic" -ldflags " \
        -s -w \
        -X main.version=0.102.x \
        -X main.buildTime=2026-03-31_06:37:02上午 \
        -X main.gitHash=3ac525e00ed486fae5b028c8714671f9c208c306" \
                -o /app/code/dist/plugins_linux_x86_64/bk-collector ./cmd/collector