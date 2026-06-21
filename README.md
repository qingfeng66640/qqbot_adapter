# QQBot Adapter - QQ Bot 适配器

对接腾讯 QQ 官方机器人（小龙虾 Bot），使用 AppID + AppSecret 认证，通过 WebSocket OpCode 网关接收消息，通过 REST API 发送消息。

## 声明

腾讯 QQ 官方机器人禁止接入AIGC功能，该插件只是用于学习用途，作者不提倡用户进行AIGC的接入，否则导致的后果由用户承担

## 架构设计

本插件采用完全自定义传输层的 **BaseAdapter 继承模式**，不依赖 mofox_wire 的自动 WebSocket 管理，自行实现 QQ 官方 OpCode 网关协议。

### 核心组件

| 文件                 | 职责                                                         |
| -------------------- | ------------------------------------------------------------ |
| `plugin.py`          | QQBotAdapter 主类，管理插件生命周期与组件协调                |
| `gateway.py`         | WebSocket OpCode 网关连接，含握手/心跳/断线恢复/intents 降级 |
| `token_manager.py`   | access_token 获取与后台定时刷新                              |
| `message_handler.py` | QQ Dispatch 事件 → MessageEnvelope 转换                      |
| `send_handler.py`    | MessageEnvelope → QQ REST API 发送，含被动回复优化           |
| `config.py`          | 4-section 配置定义（Bot/连接/功能特性）                      |

## 项目结构

```
qqbot_adapter/
├── __init__.py              # 包标识
├── manifest.json            # 插件清单
├── config.py                # 配置定义
├── plugin.py                # 主插件文件（BaseAdapter 实现）
├── gateway.py               # WebSocket 网关连接管理
├── token_manager.py         # Token 获取与刷新
├── message_handler.py       # 入站消息转换
└── send_handler.py          # 出站消息发送
```

## 快速开始

### 1. 获取凭证

前往 [QQ 开放平台](https://q.qq.com/) 创建机器人应用，获取：

- **AppID**: 应用 ID
- **AppSecret**: 应用密钥

### 2. 创建配置文件

在 `config/plugins/qqbot_adapter/` 下创建 `config.toml`：

```toml
[bot]
app_id = "你的AppID"
app_secret = "你的AppSecret"
bot_name = "我的机器人"

[connection]
env = "sandbox"           # sandbox(沙箱，无需IP白名单) / production(正式)
intents = 33554432        # 33554432 = GROUP_AND_C2C_EVENT（群聊@+单聊）
shard_count = 1

[features]
enable_group_message = true
enable_c2c_message = true

[plugin]
enabled = true
```

### 3. 启动

插件随 Neo-MoFox 自动加载，日志中看到以下输出即表示成功：

```
QQ Bot 适配器正在启动...
QQ Bot access_token 获取成功，有效期 7200 秒
获取网关地址成功: wss://...
Ready! Bot: 我的机器人 (id=xxx)
QQ Bot 适配器已就绪
```

## 配置详解

### [bot] - Bot 身份配置

| 参数         | 类型   | 说明                         |
| ------------ | ------ | ---------------------------- |
| `app_id`     | string | QQ 开放平台 AppID            |
| `app_secret` | string | QQ 开放平台 AppSecret        |
| `bot_name`   | string | Bot 显示名称（默认 "QQBot"） |

### [connection] - 连接配置

| 参数                     | 类型   | 默认值      | 说明                                   |
| ------------------------ | ------ | ----------- | -------------------------------------- |
| `env`                    | string | `"sandbox"` | 环境：sandbox(沙箱) / production(正式) |
| `intents`                | int    | `33554432`  | 事件订阅位掩码                         |
| `shard_count`            | int    | `1`         | 分片总数（不涉及分片固定为 1）         |
| `reconnect_interval`     | float  | `5.0`       | 基础重连间隔（秒），实际使用指数退避   |
| `max_reconnect_attempts` | int    | `0`         | 最大重连次数（0=无限）                 |

> **Intents 说明**:
>
> - `33554432` = GROUP_AND_C2C_EVENT（群聊@消息 + 单聊消息）
> - 鉴权失败时插件会自动逐级降级 intents 探测实际权限

### [features] - 功能特性配置

#### 群聊/私聊黑白名单

```toml
[features]
# 群聊黑名单模式：屏蔽列表中的群聊
group_list_type = "blacklist"
group_list = ["群openid1", "群openid2"]

# 私聊白名单模式：只接收列表中用户的消息
private_list_type = "whitelist"
private_list = ["用户openid1"]

# 全局封禁：这些用户的所有消息都会被忽略
ban_user_id = ["违规用户openid"]
```

| 参数                   | 类型   | 默认值        | 说明                |
| ---------------------- | ------ | ------------- | ------------------- |
| `group_list_type`      | string | `"blacklist"` | 群聊过滤模式        |
| `group_list`           | list   | `[]`          | 群聊 openid 列表    |
| `private_list_type`    | string | `"blacklist"` | 私聊过滤模式        |
| `private_list`         | list   | `[]`          | 用户 openid 列表    |
| `ban_user_id`          | list   | `[]`          | 全局封禁用户 openid |
| `enable_group_message` | bool   | `true`        | 启用群聊消息        |
| `enable_c2c_message`   | bool   | `true`        | 启用单聊消息        |

#### 常见场景配置

**个人机器人**（只服务特定群和用户）：

```toml
group_list_type = "whitelist"
private_list_type = "whitelist"
group_list = ["自己的群openid"]
private_list = ["自己的openid"]
```

**群管机器人**（屏蔽捣乱用户）：

```toml
ban_user_id = ["捣乱用户openid1", "捣乱用户openid2"]
```

**公开服务**（无限制）：

```toml
group_list_type = "blacklist"
private_list_type = "blacklist"
# 名单留空即可
```

### 被动回复机制

收到消息 5 分钟内回复时，自动使用被动回复（带上 `msg_id`），绕过主动消息限频。超时后降级为主动消息发送。

## 协议支持

完整实现 QQ Bot OpCode 网关协议：

| OpCode | 名称            | 说明                                         |
| ------ | --------------- | -------------------------------------------- |
| 10     | Hello           | 握手，获取心跳间隔                           |
| 2      | Identify        | 认证，携带 token + intents                   |
| 0      | Dispatch        | 事件分发（READY / 消息事件等）               |
| 1      | Heartbeat       | 心跳发送                                     |
| 11     | Heartbeat ACK   | 心跳确认                                     |
| 6      | Resume          | 断线恢复（保留 session）                     |
| 7      | Reconnect       | 服务端要求重连                               |
| 9      | Invalid Session | 会话无效（触发 intents 降级或重新 Identify） |

## 特性

- **intents 自动降级**: 鉴权失败时按优先级逐位移除 intents，自动探测机器人实际权限
- **指数退避重连**: `min(30s, 1.5s × 2^attempt) + random(0, 1.5s)` 防惊群
- **Token 自动刷新**: 过期前 60 秒自动获取新 token，利用新旧 token 共存窗口无间断运行
- **/gateway fallback**: 优先调用 /gateway 接口获取 WSS 地址，失败时降级到硬编码地址
- **被动回复优先**: 5 分钟窗口内使用被动回复，绕过主动消息限频
- **黑白名单过滤**: 支持群聊/私聊黑白名单和全局封禁列表

## 注意事项

1. **沙箱环境**: 开发测试时建议使用 `env = "sandbox"`，无需配置 IP 白名单
2. **正式环境**: 上线前需在 QQ 开放平台配置服务器 IP 白名单，并将 `env` 改为 `"production"`
3. **Intents 权限**: 需要在 QQ 开放平台中订阅对应的事件权限，否则 WebSocket 鉴权会失败
4. **platform 标识**: 使用 `"qqbot"` 而非 `"qq"`，避免与 OneBotAdapter 冲突导致消息路由错误

## 依赖

- Python >= 3.11
- `websockets` - WebSocket 客户端
- `httpx` - HTTP 客户端（Token 获取 + REST API 调用）
- `mofox-wire` - MessageEnvelope 数据结构

---

**作者**: qf  
**版本**: 1.0.0  
**许可证**: GPL-3.0
