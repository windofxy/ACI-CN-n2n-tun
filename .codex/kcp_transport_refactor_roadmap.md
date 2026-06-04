# KCP 传输改造最小路线图

## 总目标

针对当前分支中：

- 控制面与业务面共用单一 KCP 会话
- 内层 TCP 也可能被塞进外层 KCP

带来的控制面抖动、业务短时黑洞、应用层 TCP timeout/RST 等问题，分阶段进行最小风险改造。

## 总体顺序

建议严格按以下顺序推进：

1. KCP 调参与日志验证
2. 控制面/业务面双 KCP 会话
3. 内层 TCP/UDP 分流

不要把三步合并一次做完，否则很难判断收益与回归来源。

---

## 第 1 步：KCP 调参

### 目标

先降低 KCP 队列积压、恢复突刺、业务拥塞对时延的放大效应。

### 建议改动

仅调整以下参数：

- 打开 KCP 拥塞控制
- `snd/rcv wnd` 缩小到 `96`
- `resend` 保持 `2`
- 不修改 KCP MTU

### 建议修改位置

- `src/kcp_bridge.c`
  - `n2n_kcp_configure()`

### 预期效果

- 队列膨胀减轻
- 恢复突刺减弱
- 控制面与业务面拥塞症状减轻

### 本步先不要做

- 不改双 KCP 会话
- 不改 hint 协议
- 不改 fallback 判定语义

### 验证重点

- `KCP on_timeout` / `dead_link` 相关日志频率
- `deferring TCP fallback...` 是否减少
- 游戏转发器 TCP timeout 是否缓解

---

## 第 2 步：控制面/业务面双 KCP 会话

### 目标

把控制面与业务 `n2n_packet` 从同一条 KCP 队列中拆开，避免业务拥塞污染控制面。

### 需要新增的概念

#### edge 侧

在 `n2n_edge_t` 中增加：

- `sn_kcp_ctrl`
- `sn_kcp_data`
- `sn_kcp_ctrl_confirmed`
- `sn_kcp_data_confirmed`
- `current_supernode_kcp_channel`

#### supernode 侧

将：

- `udp_kcp_connections`

拆成：

- `udp_kcp_ctrl_connections`
- `udp_kcp_data_connections`

### 需要新增的语义

- `CTRL` channel
  - `REGISTER_SUPER`
  - `REGISTER_SUPER_ACK`
  - `REGISTER_SUPER_NAK`
  - `QUERY_PEER`
  - `PEER_INFO`
  - `PING/PONG`
- `DATA` channel
  - `n2n_packet`

### 需要优先改的函数

#### edge 侧

- `src/edge_utils.c`
  - `send_register_super()`
  - `send_query_peer()`
  - `send_unregister_super()`
  - `sendto_fd()` / `sendto_sock()`
- `src/kcp_bridge.c`
  - `n2n_kcp_edge_send*`
  - `n2n_kcp_edge_input*`

#### supernode 侧

- `src/sn_utils.c`
  - `sendto_sock()`
  - `sendto_peer()`
  - `try_forward()`
  - `try_broadcast()`
  - `REGISTER_SUPER_ACK / NAK / PEER_INFO / PONG` 发送点
- `src/kcp_bridge.c`
  - `n2n_kcp_sn_send*`
  - `n2n_kcp_sn_process_input*`

### 会话建立策略

建议第一版：

- 启动/注册时只主动点火 `ctrl KCP`
- 第一次业务 `n2n_packet` 发送时懒建立 `data KCP`

### fallback 语义

第一版建议：

- supernode 控制面存活性只看 `ctrl KCP`
- `data KCP` 坏了不直接触发 TCP fallback

### 本步先不要做

- 不改内层 TCP/UDP 分流
- 不改 `PACKET` 协议结构
- 不改 MTU

### 验证重点

- `REGISTER_SUPER_ACK` / `PEER_INFO` 是否更稳定
- `wrong or old cookie` 是否显著减少
- `deferring TCP fallback...` 是否更少、更合理

---

## 第 3 步：内层 TCP/UDP 分流

### 目标

避免内层 TCP 再走外层 KCP，使：

- 内层 TCP -> raw UDP
- 内层 UDP -> KCP

### 最小协议扩展

给 `n2n_PACKET` 增加或复用一个外层 transport hint，表达：

- `AUTO`
- `INNER_TCP`
- `INNER_UDP`

### 源 edge 的判断位置

- `src/edge_utils.c`
  - 从 TAP/Wintun 收到以太帧并封装 `n2n_PACKET_t` 的路径

第一版建议只做：

- IPv4 TCP -> `INNER_TCP`
- IPv4 UDP -> `INNER_UDP`
- 其他 -> `AUTO`
- IPv6 -> 暂时 `AUTO`

### supernode 的执行位置

- `src/sn_utils.c`
  - `try_forward()`
  - `sendto_peer()`

根据 hint 决定：

- `INNER_TCP` -> `FORCE_RAW_UDP`
- `INNER_UDP` -> `PREFER_KCP`
- `AUTO` -> 旧逻辑

### 需要改的核心函数

- `src/edge_utils.c`
  - 业务帧解析与 `encode_PACKET()`
- `src/sn_utils.c`
  - `try_forward()`
  - `sendto_peer()`
  - 业务 transport policy 选择逻辑
- `include/n2n_typedefs.h`
  - `n2n_PACKET_t` 或相关 flags/options 定义

### 兼容性策略

要求新 hint：

- 对旧 supernode 可忽略
- 对旧 edge 可回退为 `AUTO`

优先选择：

- 老版本能跳过或忽略的承载方式

避免直接破坏旧版固定解码布局。

### 本步完成后的效果预期

- 应用层 TCP timeout/RST 显著减少
- RPCN/转发器这类 TCP 敏感业务更稳
- 内层 UDP 仍可利用 KCP

---

## 不建议一开始就碰的点

以下改动建议放后面，避免同时引入太多变量：

- KCP MTU 上调
- IPv6 内层协议深解析
- 按端口做细粒度分流
- 同时重构 edge-to-edge P2P 控制逻辑
- 同时调整 TCP fallback 总体策略

---

## 每步完成后的验收建议

### 第 1 步后

- 看 timeout、黑洞窗口是否缩短
- 看 `on_timeout` / `dead_link` 是否降低

### 第 2 步后

- 看控制面日志是否变稳
- 看 `REGISTER_SUPER_ACK` 是否不再被业务拥塞拖慢

### 第 3 步后

- 看应用层 TCP 错误是否明显下降
- 看业务转发器中的 TCP timeout 是否显著减少

---

## 最终形态

理想完成态为：

- `ctrl KCP`
  - supernode 控制面专用
- `data KCP`
  - 主要承载 inner UDP 业务
- `raw UDP`
  - 承载 inner TCP 业务
- `TCP fallback`
  - 只作为 supernode 控制面兜底，不作为常态业务承载设计
