# QQBot Adapter 插件代码审查报告

**审查日期**: 2026-06-20  
**审查范围**: `plugins/qqbot_adapter/` 全部文件  
**插件版本**: 1.0.0

---

## 一、整体概要

| 项目 | 状态 |
|---|---|
| ruff 静态检查 | PASS - 无任何 lint 错误 |
| 目录结构完整性 | PASS - 7 个核心文件齐全 |
| 类型注解覆盖率 | PASS - 所有函数参数/返回值均有类型注解 |
| 文档字符串覆盖率 | PASS - 每个模块/类/函数均有 docstring |
| 日志调用规范 | PASS - 全部使用 f-string 格式 |
| OneBotAdapter 兼容性 | PASS - `platform="qqbot"` 避免与 OneBot 的 `"qq"` 冲突 |

---

## 二、逐文件审查

### 2.1 `__init__.py`
- 单行模块标识，简洁合规

### 2.2 `manifest.json`
- 声明了 `adapter` 组件类型，入口 `plugin.py`
- **注意**: 缺少 `python_dependencies` 字段（OneBot 的 manifest 有声明）。虽然 `websockets` 和 `httpx` 已在主项目中作为传递依赖可用，但显式声明更规范

### 2.3 `config.py`
- 4 个配置段：`plugin`, `bot`, `connection`, `features`
- `app_secret` 正确使用 `input_type="password"`
- 所有字段配有 `description/label/placeholder/hint/tag`
- 数值字段配有 `ge/le/step` 约束
- **合格**

### 2.4 `token_manager.py`
- 完整实现 access_token 获取与定时刷新
- `expires_in` 字符串→int 转换已修复
- 刷新策略：过期前 60s 进入刷新窗口
- `ensure_token()` 作为 token 提供者接口供 gateway 和 send_handler 使用
- **合格**

### 2.5 `gateway.py`
- 完整 OpCode 协议实现（10/2/0/1/11/6/7/9）
- intents 自动降级探测机制
- `/gateway` 接口 fallback 到硬编码 WSS 地址
- 指数退避 + jitter 重连策略
- Resume 优先重连（有 session_id 时）
- Identify 携带 `properties` 字段（`$os/$browser/$device`）
- **合格**

### 2.6 `message_handler.py`
- 正确处理 `C2C_MESSAGE_CREATE` 和 `GROUP_AT_MESSAGE_CREATE`
- `from_user()` 有 nickname fallback（`QQ用户_xxx` / `群成员_xxx`）
- 附件下载并转 base64
- 黑白名单/封禁过滤
- metadata 中存储 `qq_event_id/qq_event_type/qq_group_openid/qq_user_openid` 用于被动回复
- **合格**

### 2.7 `send_handler.py`
- 被动回复优先策略（5 分钟内使用 `msg_id`）
- 文本消息 `msg_type=0`，富媒体 `msg_type=7`
- 401 token 过期自动刷新重试
- 429 限频记录日志
- **合格**

### 2.8 `plugin.py`
- `QQBotAdapter(BaseAdapter)` 不传 `transport`，完全自定义传输
- `on_adapter_loaded()` 按序初始化 TokenManager → SendHandler → GatewayConnection
- `_send_platform_message()` 重写为调用 SendHandler
- `health_check()` / `reconnect()` / `get_bot_info()` 均自定义实现
- `platform="qqbot"` 避免与 OneBot 冲突
- **合格**

---

## 三、发现的问题

### 低风险

| 编号 | 文件 | 问题 | 建议 |
|---|---|---|---|
| 1 | `manifest.json` | 未声明 `python_dependencies`，依赖的 `websockets`/`httpx` 虽已作为传递依赖可用，但显式声明更规范 | 建议添加 |
| 2 | `config.py:37` | `app_id` 字段 `placeholder` 拼写正确，但建议添加 `tag="user"` 后的更多验证 | 可忽略 |
| 3 | 整个插件 | 缺少默认配置文件 `config/plugins/qqbot_adapter/config.toml` | 用户需手动创建 |

### 无高/中风险问题

---

## 四、与 OneBotAdapter 对照

| 特性 | OneBotAdapter | QQBotAdapter | 评价 |
|---|---|---|---|
| 传输层 | `mofox_wire WebSocket`（自动） | 原生 `websockets` + 自建 OpCode | 均正确实现 |
| 消息转换 | `MessageHandler` | `MessageHandler` | 模式一致 |
| 发送处理 | `SendHandler` | `SendHandler` | 模式一致 |
| 配置验证 | `_validate_bot_identity()` | `_validate_bot_identity()` | 模式一致 |
| platform 声明 | `"qq"` | `"qqbot"` | **正确分离** |
| manifest | 含 `python_dependencies` | **缺少** | 建议补齐 |

---

## 五、总体评价

**代码质量: 优秀**  
插件架构清晰，模块职责分明，完整实现了 QQ 官方 Bot 的 WebSocket OpCode 协议。参照了 OneBotAdapter 的 Handler 模式，同时正确避开 platform 冲突。所有日志调用符合 Neo-MoFox Logger 规范，ruff 检查零错误。**可以提交。**