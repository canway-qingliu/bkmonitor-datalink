// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package dorisclient

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Options 描述一个 Doris FE 只读连接。凭据只存在于 UQ 侧配置，
// 不随 Trace 读路由快照发布（读写契约 §5.2.1）。
type Options struct {
	ClusterID      string
	Host           string
	Port           int
	Username       string
	Password       string
	MaxOpenConns   int
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	// MaxRows 是单次查询允许返回的最大行数，超预算直接失败，不截断。
	MaxRows int
}

const (
	defaultConnectTimeout = 3 * time.Second
	defaultQueryTimeout   = 10 * time.Second
	defaultMaxRows        = 10000
)

// Column 是结果集的列描述，Type 为驱动上报的数据库类型名。
type Column struct {
	Name string
	Type string
}

// Result 保留列顺序、列类型与已规范化的行值。
type Result struct {
	Columns []Column
	Rows    []map[string]any
}

// TypeOf 返回指定列的类型名，列不存在时返回空串。
func (r Result) TypeOf(name string) string {
	for _, column := range r.Columns {
		if column.Name == name {
			return column.Type
		}
	}
	return ""
}

// Instance 是受控的 Doris 只读连接。它不暴露任意 SQL 执行入口：
// 语句必须通过 ValidateStatement 的只读形态检查。
type Instance struct {
	clusterID    string
	db           *sql.DB
	queryTimeout time.Duration
	maxRows      int
}

// New 建立连接池；不会立即建连，首次查询时按 ConnectTimeout 连接。
func New(opt Options) (*Instance, error) {
	if strings.TrimSpace(opt.ClusterID) == "" || strings.TrimSpace(opt.Host) == "" || opt.Port <= 0 {
		return nil, fmt.Errorf("Doris 连接配置缺少集群、地址或端口")
	}
	if opt.Username == "" {
		return nil, fmt.Errorf("Doris 连接配置缺少只读用户名")
	}
	connectTimeout := opt.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = defaultConnectTimeout
	}
	queryTimeout := opt.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = defaultQueryTimeout
	}
	maxRows := opt.MaxRows
	if maxRows <= 0 {
		maxRows = defaultMaxRows
	}
	maxOpenConns := opt.MaxOpenConns
	if maxOpenConns <= 0 {
		maxOpenConns = 4
	}

	cfg := mysql.NewConfig()
	cfg.User = opt.Username
	cfg.Passwd = opt.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", opt.Host, opt.Port)
	cfg.Timeout = connectTimeout
	cfg.ReadTimeout = queryTimeout
	cfg.WriteTimeout = queryTimeout
	// parseTime=false：DATE/BIGINT 一律按文本/整数返回，行规范化在 Query 内完成。
	cfg.ParseTime = false
	// interpolateParams 保持 false，使用服务端 prepared statement 做真正的位置参数绑定。
	cfg.InterpolateParams = false
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	// 关闭本地 LOAD DATA 能力，只读客户端不需要。
	cfg.AllowAllFiles = false

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("Doris 连接配置非法: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &Instance{clusterID: opt.ClusterID, db: db, queryTimeout: queryTimeout, maxRows: maxRows}, nil
}

// ClusterID 返回该连接声明的集群身份，供路由比对使用。
func (i *Instance) ClusterID() string {
	return i.clusterID
}

// Close 释放连接池。
func (i *Instance) Close() error {
	if i == nil || i.db == nil {
		return nil
	}
	return i.db.Close()
}

// Query 执行一条受控只读语句。语句只允许 SELECT/WITH 单语句形态，
// 参数一律使用绑定值，不做字符串拼接。
func (i *Instance) Query(ctx context.Context, statement string, args ...any) (Result, error) {
	if err := ValidateStatement(statement); err != nil {
		return Result{}, err
	}
	queryCtx := ctx
	if i.queryTimeout > 0 {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, i.queryTimeout)
		defer cancel()
	}
	rows, err := i.db.QueryContext(queryCtx, statement, args...)
	if err != nil {
		return Result{}, fmt.Errorf("Doris 查询失败: %w", err)
	}
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	if err != nil {
		return Result{}, fmt.Errorf("Doris 结果列读取失败: %w", err)
	}
	rawTypes, err := rows.ColumnTypes()
	if err != nil {
		return Result{}, fmt.Errorf("Doris 结果列类型读取失败: %w", err)
	}
	columns := make([]Column, len(names))
	for index, name := range names {
		columns[index] = Column{Name: name}
		if index < len(rawTypes) {
			columns[index].Type = rawTypes[index].DatabaseTypeName()
		}
	}

	scanned := make([]any, len(names))
	holders := make([]any, len(names))
	for index := range scanned {
		holders[index] = &scanned[index]
	}
	result := Result{Columns: columns, Rows: make([]map[string]any, 0, 16)}
	for rows.Next() {
		if err := rows.Scan(holders...); err != nil {
			return Result{}, fmt.Errorf("Doris 结果行读取失败: %w", err)
		}
		if len(result.Rows) >= i.maxRows {
			return Result{}, fmt.Errorf("Doris 结果超过行预算 %d", i.maxRows)
		}
		row := make(map[string]any, len(names))
		for index, name := range names {
			value, err := normalizeValue(scanned[index])
			if err != nil {
				return Result{}, fmt.Errorf("Doris 列 %s 值不受支持: %w", name, err)
			}
			row[name] = value
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return Result{}, fmt.Errorf("Doris 结果集异常: %w", err)
	}
	return result, nil
}

// normalizeValue 只放行可安全跨层传递的标量；VARIANT/JSON 文本保持字符串，
// 由 profile 层按列语义解码，客户端不猜测列含义。
func normalizeValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return string(typed), nil
	case string:
		return typed, nil
	case int64:
		return typed, nil
	case float64:
		return typed, nil
	case bool:
		return typed, nil
	default:
		return nil, fmt.Errorf("非标量值 %T", value)
	}
}

var forbiddenStatementFragments = []string{";", "--", "/*", "*/", "#"}

// ValidateStatement 限制只读单语句形态：必须是 SELECT 或 WITH 开头，
// 且不含注释与多语句分隔符。它用于防呆，不替代只读账号权限。
func ValidateStatement(statement string) error {
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" {
		return fmt.Errorf("Doris 语句为空")
	}
	for _, fragment := range forbiddenStatementFragments {
		if strings.Contains(trimmed, fragment) {
			return fmt.Errorf("Doris 语句含禁用片段 %q", fragment)
		}
	}
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT ") && !strings.HasPrefix(upper, "WITH ") {
		return fmt.Errorf("Doris 语句只允许 SELECT/WITH")
	}
	return nil
}
