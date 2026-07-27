# Go 语言代码可读性与可维护性指南

## 目录

- [1. 改进变量命名](#1-改进变量命名)
- [2. 避免条件链](#2-避免条件链)
- [3. 减少嵌套逻辑](#3-减少嵌套逻辑)
- [4. 拆分大型方法](#4-拆分大型方法)
- [5. 错误处理优化](#5-错误处理优化)
- [6. 添加文档注释](#6-添加文档注释)
- [7. 接口设计与依赖注入](#7-接口设计与依赖注入)
- [8. 综合重构建议](#8-综合重构建议)

---

## 1. 改进变量命名

### 问题

抽象或模糊的变量名让人难以理解代码的用途。

### 不良示例

```go
func logPersonAge(a string, b int, c bool) {
    if c {
        fmt.Printf("%s is %d years old.\n", a, b)
    } else {
        fmt.Printf("%s does not want to reveal their age.\n", a)
    }
}
```

### 改进后

```go
func logPersonAge(name string, age int, revealAge bool) {
    if revealAge {
        fmt.Printf("%s is %d years old.\n", name, age)
    } else {
        fmt.Printf("%s does not want to reveal their age.\n", name)
    }
}
```

### Go 命名约定

| 类型 | 约定 | 示例 |
|------|------|------|
| 包名 | 小写，单数，简短 | `http`, `json`, `user` |
| 导出变量/函数 | 大写开头，驼峰 | `GetUser`, `ConfigPath` |
| 私有变量/函数 | 小写开头，驼峰 | `validateInput`, `cache` |
| 接口名 | 以 `er` 结尾 | `Reader`, `Writer`, `Closer` |
| 接收器 | 简短（1-2字母） | `u *User`, `s *Service` |
| 常量 | 大写或驼峰 | `MaxRetries`, `timeout` |

### 原则

- 变量名应在上下文中清晰表达**用途**
- 避免单字母（除循环索引 `i`, `j`, `k`）
- 布尔变量使用 `is`、`has`、`can`、`should` 前缀
- 包名应简短且有意义，避免 `utils`、`common` 等泛化名称

---

## 2. 避免条件链

### 问题

过长的 `if...else` 链难以阅读和维护，扩展时容易出错。

### 不良示例

```go
func animalSound(animalType string) string {
    if animalType == "dog" {
        return "Woof!"
    } else if animalType == "cat" {
        return "Meow!"
    } else if animalType == "bird" {
        return "Tweet!"
    } else {
        return "Unknown animal"
    }
}
```

### 改进后（使用 Map）

```go
var animalSounds = map[string]string{
    "dog":  "Woof!",
    "cat":  "Meow!",
    "bird": "Tweet!",
}

func animalSound(animalType string) string {
    if sound, ok := animalSounds[animalType]; ok {
        return sound
    }
    return "Unknown animal"
}
```

### 改进后（使用接口+多态）

```go
// 定义接口
type Animal interface {
    Speak() string
}

// 具体实现
type Dog struct{}
func (d Dog) Speak() string { return "Woof!" }

type Cat struct{}
func (c Cat) Speak() string { return "Meow!" }

type Bird struct{}
func (b Bird) Speak() string { return "Tweet!" }

// 工厂映射
var animalFactories = map[string]func() Animal{
    "dog":  func() Animal { return Dog{} },
    "cat":  func() Animal { return Cat{} },
    "bird": func() Animal { return Bird{} },
}

func animalSound(animalType string) string {
    factory, ok := animalFactories[animalType]
    if !ok {
        return "Unknown animal"
    }
    return factory().Speak()
}
```

---

## 3. 减少嵌套逻辑

### 问题

深度嵌套的结构会使代码难以理解和修改。

### 不良示例

```go
func determineAccess(userRole string, hasPermission bool, isActive bool) string {
    if userRole == "admin" {
        if hasPermission {
            if isActive {
                return "Active admin account with full access."
            } else {
                return "Inactive admin account."
            }
        } else {
            return "Admin account lacks necessary permissions."
        }
    } else {
        return "Access denied."
    }
}
```

### 改进后（使用卫语句）

```go
func determineAccess(userRole string, hasPermission bool, isActive bool) string {
    if userRole != "admin" {
        return "Access denied."
    }
    if !hasPermission {
        return "Admin account lacks necessary permissions."
    }
    if !isActive {
        return "Inactive admin account."
    }
    return "Active admin account with full access."
}
```

### 改进后（使用错误类型）

```go
type AccessError string

func (e AccessError) Error() string {
    return string(e)
}

const (
    ErrAccessDenied        = AccessError("access denied")
    ErrNoPermission        = AccessError("admin account lacks necessary permissions")
    ErrInactiveAccount     = AccessError("inactive admin account")
)

func determineAccess(userRole string, hasPermission bool, isActive bool) (string, error) {
    if userRole != "admin" {
        return "", ErrAccessDenied
    }
    if !hasPermission {
        return "", ErrNoPermission
    }
    if !isActive {
        return "", ErrInactiveAccount
    }
    return "Active admin account with full access.", nil
}
```

### 原则

- **卫语句**优先处理异常和边界条件
- 每层嵌套应处理**一个明确的关注点**
- 嵌套层级不超过 **3 层**
- 深层嵌套考虑抽取为独立函数

---

## 4. 拆分大型方法

### 问题

单个函数执行多个任务时，难以理解、测试和复用。

### 不良示例

```go
func processOrder(order *Order) error {
    if order == nil || len(order.Items) == 0 {
        return errors.New("order is invalid")
    }

    // 计算总价
    var totalPrice float64
    for _, item := range order.Items {
        totalPrice += item.Price * float64(item.Quantity)
    }
    order.TotalPrice = totalPrice

    // 更新状态
    if totalPrice > 0 {
        order.Status = "Processed"
    } else {
        order.Status = "Pending"
    }

    // 发送通知
    fmt.Printf("Order for customer %s has been processed. Total price: %.2f\n",
        order.CustomerName, totalPrice)

    return nil
}
```

### 改进后（拆分为多个函数）

```go
func processOrder(order *Order) error {
    if err := validateOrder(order); err != nil {
        return err
    }

    totalPrice := calculateTotalPrice(order)
    updateOrderStatus(order, totalPrice)
    printOrderSummary(order, totalPrice)

    return nil
}

func validateOrder(order *Order) error {
    if order == nil {
        return errors.New("order is nil")
    }
    if len(order.Items) == 0 {
        return errors.New("order has no items")
    }
    return nil
}

func calculateTotalPrice(order *Order) float64 {
    var total float64
    for _, item := range order.Items {
        total += item.Price * float64(item.Quantity)
    }
    order.TotalPrice = total
    return total
}

func updateOrderStatus(order *Order, totalPrice float64) {
    if totalPrice > 0 {
        order.Status = "Processed"
    } else {
        order.Status = "Pending"
    }
}

func printOrderSummary(order *Order, totalPrice float64) {
    fmt.Printf("Order for customer %s has been processed. Total price: %.2f\n",
        order.CustomerName, totalPrice)
}
```

### 原则

- 每个函数只做**一件事**（单一职责）
- 函数名应准确描述其**职责**
- 单个函数建议不超过 **30 行**
- 拆分后的函数便于**单独测试**
- 使用**接收器方法**组织相关功能

---

## 5. 错误处理优化

### 问题

Go 中常见的 `if err != nil` 重复模式会让代码变得臃肿。

### 不良示例

```go
func getUserData(userID int) (*User, error) {
    resp, err := http.Get(fmt.Sprintf("/users/%d", userID))
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }

    var user User
    err = json.Unmarshal(body, &user)
    if err != nil {
        return nil, err
    }

    return &user, nil
}
```

### 改进后（包装错误）

```go
func getUserData(userID int) (*User, error) {
    resp, err := http.Get(fmt.Sprintf("/users/%d", userID))
    if err != nil {
        return nil, fmt.Errorf("failed to fetch user %d: %w", userID, err)
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, fmt.Errorf("failed to read response body: %w", err)
    }

    var user User
    if err := json.Unmarshal(body, &user); err != nil {
        return nil, fmt.Errorf("failed to unmarshal user data: %w", err)
    }

    return &user, nil
}
```

### 改进后（使用辅助函数）

```go
func getUserData(userID int) (*User, error) {
    body, err := fetchUserResponse(userID)
    if err != nil {
        return nil, err
    }
    return parseUserResponse(body)
}

func fetchUserResponse(userID int) ([]byte, error) {
    resp, err := http.Get(fmt.Sprintf("/users/%d", userID))
    if err != nil {
        return nil, fmt.Errorf("fetch failed: %w", err)
    }
    defer resp.Body.Close()

    return ioutil.ReadAll(resp.Body)
}

func parseUserResponse(body []byte) (*User, error) {
    var user User
    if err := json.Unmarshal(body, &user); err != nil {
        return nil, fmt.Errorf("parse failed: %w", err)
    }
    return &user, nil
}
```

### Go 错误处理最佳实践

| 实践 | 说明 |
|------|------|
| 始终检查错误 | 不要忽略 `err` |
| 包装错误 | 使用 `fmt.Errorf("...: %w", err)` 添加上下文 |
| 自定义错误类型 | 实现 `error` 接口，提供更多信息 |
| 错误在前，成功在后 | 先判断错误，再处理正常逻辑 |
| 使用 `errors.Is` / `errors.As` | 判断错误类型 |

---

## 6. 添加文档注释

### Go 文档注释规范

```go
// Package user provides user management functionality.
//
// It includes operations for creating, retrieving, updating, and
// deleting user records in the system.
package user

// User represents a registered user in the system.
type User struct {
    ID       int       `json:"id"`
    Username string    `json:"username"`
    Email    string    `json:"email"`
    CreatedAt time.Time `json:"created_at"`
}

// GetUser retrieves a user by their ID.
//
// It returns an error if the user does not exist or if the database
// query fails. Use ErrUserNotFound to check for non-existent users.
//
// Example:
//
//     user, err := userService.GetUser(ctx, 123)
//     if errors.Is(err, ErrUserNotFound) {
//         // handle not found
//     }
func (s *UserService) GetUser(ctx context.Context, id int) (*User, error) {
    // ...
}
```

### 文档注释原则

- **包注释**：放在 `doc.go` 或主文件开头
- **导出类型**：说明用途和字段含义
- **导出函数**：说明功能、参数、返回值和错误
- **提供示例**：用 `Example:` 展示用法
- **错误说明**：明确说明可能返回的错误类型

---

## 7. 接口设计与依赖注入

### 问题

紧耦合的代码难以测试和扩展。

### 不良示例

```go
type UserService struct{}

func (s *UserService) GetUser(id int) (*User, error) {
    db := sql.Open("mysql", "dsn")
    // 直接依赖具体数据库实现
    return queryUser(db, id)
}
```

### 改进后（面向接口）

```go
// 定义接口
type UserRepository interface {
    GetUser(ctx context.Context, id int) (*User, error)
    SaveUser(ctx context.Context, user *User) error
}

// 依赖注入
type UserService struct {
    repo UserRepository
}

func NewUserService(repo UserRepository) *UserService {
    return &UserService{repo: repo}
}

func (s *UserService) GetUser(ctx context.Context, id int) (*User, error) {
    return s.repo.GetUser(ctx, id)
}

// 具体实现（MySQL）
type MySQLUserRepository struct {
    db *sql.DB
}

func (r *MySQLUserRepository) GetUser(ctx context.Context, id int) (*User, error) {
    // MySQL 具体实现
}

// 便于测试
type mockUserRepository struct {
    users map[int]*User
}

func (m *mockUserRepository) GetUser(ctx context.Context, id int) (*User, error) {
    if user, ok := m.users[id]; ok {
        return user, nil
    }
    return nil, ErrUserNotFound
}
```

### 接口设计原则

- **小而精**：接口应包含少量方法（通常 1-3 个）
- **以 `er` 结尾**：`Reader`, `Writer`, `Closer`
- **在使用方定义**：接口由使用者定义，而非实现者
- **接受接口，返回结构体**

---

## 8. 综合重构建议

### Go 代码异味检查清单

| 问题 | 检查标准 | 改进方向 |
|------|----------|----------|
| 长函数 | > 30 行 | 拆分为多个小函数 |
| 长参数列表 | > 4 个参数 | 使用结构体参数 |
| 重复代码 | 相同逻辑 ≥ 2 次 | 抽取公共函数 |
| 过深嵌套 | > 3 层 | 使用卫语句或提前返回 |
| 魔法数字/字符串 | 硬编码值 | 定义为常量 |
| 过大结构体 | > 10 个字段 | 按职责拆分 |
| 空白标识符滥用 | `_` 忽略错误 | 正确处理错误 |

### 代码审查关注点

```go
// ✅ 好的代码特征
func (s *Service) ProcessOrder(ctx context.Context, req *ProcessRequest) (*ProcessResponse, error) {
    // 1. 参数验证（提前返回）
    if err := s.validateRequest(req); err != nil {
        return nil, fmt.Errorf("invalid request: %w", err)
    }

    // 2. 业务逻辑（清晰的步骤）
    order, err := s.createOrder(ctx, req)
    if err != nil {
        return nil, fmt.Errorf("create order failed: %w", err)
    }

    // 3. 后续处理
    if err := s.notifyOrder(ctx, order); err != nil {
        // 非关键错误，记录日志
        s.logger.Warn("notification failed", "error", err)
    }

    return &ProcessResponse{OrderID: order.ID}, nil
}
```

### 常用重构模式

| 模式 | 适用场景 | 示例 |
|------|----------|------|
| 提取函数 | 长函数 | 将计算、验证等逻辑独立 |
| 提取常量 | 魔法值 | 定义 `const MaxRetries = 3` |
| 引入参数对象 | 参数过多 | 使用 `Request` 结构体 |
| 用 Map 替代条件链 | 多个 `if-else` | 使用 `map[string]Handler` |
| 策略模式 | 算法可替换 | 定义接口，多态实现 |
| 工厂模式 | 对象创建复杂 | `NewXXX()` 构造函数 |

---

## 推荐阅读

- 《Go 语言圣经》（The Go Programming Language）
- 《Effective Go》（官方文档）
- 《Go 代码审查指南》（Go Code Review Comments）
- 《Clean Architecture in Go》

---

> **总结**：Go 语言强调简洁和清晰。好的 Go 代码应该：
> 1. **命名清晰**，符合 Go 惯例
> 2. **错误处理**明确，不忽略任何错误
> 3. **函数简短**，职责单一
> 4. **接口小巧**，便于组合
> 5. **文档完整**，特别是导出元素
> 6. **避免过度设计**，保持简单（KISS）