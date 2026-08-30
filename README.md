# astrbot_plugin_notice_server

> NoticeServer / BotNotice API 封装插件（`dscontrol` 兼容指令体系）

封装 NoticeServer（BotNotice）的 HTTP / WebSocket API，对外暴露一套与 `dscontrol` 完全对齐的 `ds` 前缀指令，用于在 QQ / Discord 群聊中管理游戏服务器：查看服务器列表与在线状态、查询版本、UUID 封禁管理、玩家登录记录查询、控制台命令下发，以及群名单同步与进群/退群事件即时上报。

- **插件名**：`astrbot_plugin_notice_server`
- **显示名**：NoticeServer API 封装（dscontrol 兼容）
- **版本**：v2.8.0
- **作者**：迪普希克

---

## 功能特性

- **WebSocket 长连接**：对接 BotNotice `/ws/bot` 命令通道，自动重连 + 心跳保活，替代旧版 HTTP 请求。
- **dscontrol 指令对齐**：`ds` 前缀，参数与功能与 dscontrol 一致。
- **UUID 封禁管理**：`ds ban` / `ds unban` / `ds banlist`，封禁前自动查记录、交互式确认，40 秒确认超时、60 秒序号选择超时。
- **玩家记录多模式查询**：按玩家名搜索、分页、时间段、玩家名 + IP 筛选。
- **服务器名自动解析**：通过 ServerId 查询 Tag 显示友好名，查不到则兜底显示原始 ServerId。
- **时间排序**：`ds records` / `ds ban` 选人 / `ds banlist` 支持按时间降序（最新在前）。
- **群名单同步（GroupWhitelist）**：QQ 群与 Discord 服务器双平台，状态硬盘持久化 + 差量版本续接。
- **进群/退群事件即时上报（MemberEventSync）**：QQ 事件 + Discord 成员事件，防抖累积后批量上报。
- **验证码消息判定**：验证码消息一律提交服务器判定，服务器未放行时静默不回复。
- **多平台白名单**：QQ 与 Discord 名单相互独立，支持群/频道白名单、个人白名单、黑名单。
- **频率限制**：`auth_control` 关闭时，`ds version` / `ds servers` 每 5 分钟最多 1 次，按用户独立计数，白名单不受限，限频时静默拒绝。

---

## 安装

将插件目录放入 AstrBot 的插件目录（如 `data/plugins/`），然后重载插件即可。

```
data/plugins/astrbot_plugin_notice_server/
├── main.py               # 插件主逻辑与指令
├── ws_client.py          # WebSocket 长连接客户端
├── group_sync.py         # 群名单同步管理
├── member_event_sync.py  # 进群/退群事件同步
├── member_guard.py       # 成员守卫
├── metadata.yaml         # 插件元数据
└── _conf_schema.json     # 配置项 Schema
```

---

## 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `server_host` | string | `127.0.0.1` | NoticeServer 服务器地址 |
| `server_port` | int | `11470` | NoticeServer / BotNotice 端口 |
| `bot_id` | string | `botA` | WebSocket BotId（对应服务端 BotTokens 的键） |
| `ws_token` | string | 空 | WebSocket Token，留空回退 `api_key` |
| `api_key` | string | 空 | 旧版 HTTP 鉴权 Token（ws_token 回退值） |
| `timeout` | int | `10` | 请求超时（秒） |
| `group_whitelist` | list | `[]` | QQ 群聊白名单（群号） |
| `group_user_whitelist` | list | `[]` | QQ 群聊-个人白名单（`群号:QQ号`） |
| `friend_whitelist` | list | `[]` | QQ 私聊白名单（QQ号） |
| `group_blacklist` | list | `[]` | QQ 群黑名单（群号） |
| `discord_group_whitelist` | list | `[]` | Discord 群聊白名单（`服务器ID` 或 `服务器ID:频道ID`） |
| `discord_group_user_whitelist` | list | `[]` | Discord 群聊-个人白名单 |
| `discord_friend_whitelist` | list | `[]` | Discord 私聊白名单（用户ID） |
| `discord_group_blacklist` | list | `[]` | Discord 群黑名单 |
| `auth_control` | bool | `true` | 鉴权控制开关（关闭时 `ds version` / `ds servers` 所有人可用） |
| `max_inline_items` | int | `7` | 展示最大内联条数，超过转聊天记录发送 |
| `sort_by_time` | bool | `true` | 时间排序开关（records / ban 选人 / banlist 按时间降序） |
| `group_sync_enabled` | bool | `false` | 群名单同步总开关 |
| `group_sync_groups` | list | `[]` | QQ 群名单同步受管群（群号） |
| `discord_sync_guilds` | list | `[]` | Discord 名单同步受管范围（`服务器ID` 或 `服务器ID:频道ID`） |
| `group_sync_interval` | int | `300` | 群名单轮询间隔（秒，最小 30） |
| `group_sync_retention` | int | `50` | 群名单变更历史保留版本数（最小 2） |
| `group_sync_persist` | bool | `true` | 群名单状态硬盘持久化开关 |
| `group_sync_state_file` | string | 空 | 状态持久化文件路径（留空自动使用插件 data 目录） |
| `member_event_sync_enabled` | bool | `false` | 群成员事件同步总开关（进群/退群即时上报） |
| `member_event_quiet_window` | int | `1` | 事件静默窗口（秒） |
| `member_event_max_window` | int | `5` | 事件最大累积窗口（秒） |

---

## 命令列表

所有命令需在配置了服务器地址、端口、Token 后使用。仅群聊可用，受白名单控制。

| 命令 | 说明 | 鉴权 |
| --- | --- | --- |
| `ds` | 显示服务器列表与在线状态 | 白名单 |
| `ds version` | 获取服务器版本号 | 无需 API 鉴权 |
| `ds servers` | 显示服务器列表（含在线人数） | 无需 API 鉴权 |
| `ds reload` | 重新加载服务器配置 | 需要 |
| `ds ban <PlayerId> [原因]` | 封禁玩家（按 UUID，自动查记录，交互式确认） | 需要 |
| `ds banlist` | 获取封禁列表（UUID 封禁） | 需要 |
| `ds unban <UUID\|玩家名\|IP>` | 解封账号（需 Y 确认） | 需要 |
| `ds records <关键词>` | 查询玩家登录记录（多模式，见下） | 需要 |
| `ds run <服务器> <命令>` | 向指定服务器发送控制台命令 | 需要 |
| `ds groupsync` | 查看群名单同步状态 | 需要 |
| `ds groupsync now` | 手动触发一次群名单轮询 | 需要 |
| `ds groupsync snapshot` | 手动发送完整快照 | 需要 |
| `ds groupsync delta [版本]` | 手动补包（默认从版本 0） | 需要 |
| `ds lookup <QQ号\|discord:ID>` | 按绑定渠道实时反查 UUID（服务端 Type 16） | 需要 |
| `ds bindings <UUID>` | 查询账号绑定渠道 | 需要 |

### `ds records` 子命令

| 子命令 | 说明 |
| --- | --- |
| `ds records <关键词>` | 按玩家名模糊搜索 |
| `ds records list [offset] [limit]` | 分页查看全部记录 |
| `ds records range <开始> <结束>` | 按时间段查询（ISO 时间） |
| `ds records filter <玩家名> [IP]` | 按玩家名 + IP 筛选 |

---

## 群名单同步（GroupWhitelist）

- 开启 `group_sync_enabled` 并配置 `group_sync_groups` 后，定期拉取受管群成员并上报差异。
- 配置 `discord_sync_guilds` 后，同步 Discord 服务器成员，与 QQ 群共用 `EWhitelistPush(Type=3)` 版本链，快照按平台合并上报（Whitelist 含 `1`=QQ / `2`=Discord）。
- 群名单状态（版本号 / 成员快照 / 差量历史）持久化到本地硬盘（`group_sync_persist`，默认开）。
- 重启 Bot 后读取上次保存的版本续接，基于最后版本计算差量推送，不从 v0 重建。
- 服务端请求完整清单时仍返回全量快照。

## 群成员事件同步（MemberEventSync）

- 开启 `member_event_sync_enabled` 后，监听受管群（`group_sync_groups`）的进群/退群通知；Discord 服务器成员加入/离开（`discord_sync_guilds`）同样即时上报。
- 防抖累积（静默 1 秒 / 最长 5 秒，可配置）后批量上报 MasterServer。
- 复用现成 `EWhitelistPush(Type=3)` 版本链，服务端无需新增命令码。
- 需同时开启 `group_sync_enabled`。

---

## 使用示例

```
ds
ds version
ds servers
ds reload
ds ban 江019 恶意TK
ds banlist
ds unban 550e8400-e29b-41d4-a716-446655440000
ds records 江019
ds records list 0 20
ds records range 2026-08-01T00:00:00 2026-08-16T00:00:00
ds records filter 江019 1.2.3.4
ds run 通用服 kill 玩家名
ds groupsync
ds groupsync now
ds lookup 123456789
ds lookup discord:987654321098765432
ds bindings 550e8400-e29b-41d4-a716-446655440000
```

---

## 权限说明

- 公开接口（`ds version` / `ds servers`）无需 API 鉴权，鉴权接口需要配置 Token。
- 所有指令在插件层面均有白名单检查，受 `group_whitelist` / `group_user_whitelist` / `friend_whitelist` 等控制。
- 黑名单群（`group_blacklist`）中任何调用均完全静默忽略。
- `auth_control` 关闭时，`ds version` / `ds servers` 启用频率限制（每 5 分钟 1 次），白名单不受限。

---

## 注意事项

- 服务器名显示依赖 `/api/server-list` 的 Tag 映射；若管理员更新后 ServerId 变为补零格式（如 `server01`），需同步更新 `ServerList.json` 的 ServerId。
- 群名单同步与成员事件同步需服务端在 `ServersConfig.json` 的 `GroupWhitelist` 中启用并配置对应群。
