# KCP 双会话分析

## 目标

将当前 supernode 通道上的单一 KCP 会话拆成两条：

- `ctrl KCP`：承载 supernode 控制面
- `data KCP`：承载业务 `n2n_packet`

这样可以避免业务流量在 KCP 队列中阻塞控制包，减少控制面与数据面的相互污染。

## 当前问题

当前实现中，以下流量可能共用同一条 supernode KCP 会话：

- `REGISTER_SUPER`
- `REGISTER_SUPER_ACK`
- `REGISTER_SUPER_NAK`
- `QUERY_PEER`
- `PEER_INFO`
- `PING/PONG`
- `n2n_packet`

结果是：

- 业务大流量会拖慢 `REGISTER_SUPER_ACK`、`PEER_INFO`
- 控制面超时判断会被业务面抖动污染
- 容易出现 `deferring TCP fallback...`、`wrong or old cookie` 之类的控制面震荡

## 最小设计

### 会话划分

- `ctrl KCP`
  - `REGISTER_SUPER`
  - `REGISTER_SUPER_ACK`
  - `REGISTER_SUPER_NAK`
  - `QUERY_PEER`
  - `PEER_INFO`
  - `PING/PONG`
- `data KCP`
  - `n2n_packet`

### edge 侧字段建议

在 `n2n_edge_t` 中引入：

- `sn_kcp_ctrl`
- `sn_kcp_data`
- `sn_kcp_ctrl_confirmed`
- `sn_kcp_data_confirmed`
- `current_supernode_kcp_channel`

建议第一版里：

- fallback 判定只看 `ctrl KCP`
- `data KCP` 只作为业务优化通道，不直接参与 supernode 控制面失联判断

### supernode 侧字段建议

将当前：

- `udp_kcp_connections`

拆成：

- `udp_kcp_ctrl_connections`
- `udp_kcp_data_connections`

这样日志、清理逻辑、后续调参都会更清楚。

## 协议与 conv 设计

双会话最关键的问题是：收到一个 KCP UDP 包时，如何判断它属于 ctrl 还是 data。

最小方案：

- 每个 supernode 对应两组 conv
  - `ctrl_conv`
  - `data_conv`
- 控制包只进入 `ctrl_conv`
- 数据包只进入 `data_conv`

这样无需修改 KCP payload 结构，只需在入 KCP 前选对 ctx，收包时按 conv 选 ctx。

## 发送路径改造建议

### edge -> supernode

当前控制包发送函数主要有：

- `send_register_super()`
- `send_query_peer()`
- `send_unregister_super()`

建议：

- 保留 `sending_supernode_control`
- 在这些函数发送前设置 `current_supernode_kcp_channel = CTRL`
- 普通业务 `PACKET` 默认设为 `DATA`

然后在 `sendto_fd()` 中根据目标是当前 supernode 且 transport 不是 TCP 的条件，选择：

- `sn_kcp_ctrl`
- `sn_kcp_data`

### supernode -> edge

当前控制回复点主要有：

- `REGISTER_SUPER_ACK`
- `REGISTER_SUPER_NAK`
- `PEER_INFO`
- `PONG`

建议给 `sendto_peer()` 引入显式的 `channel_kind` 参数：

- `CTRL`
- `DATA`

其中：

- `try_forward()` / `try_broadcast()` 对 `n2n_packet` 固定传 `DATA`
- 控制回复发送点传 `CTRL`

## 收包路径改造建议

### edge 收 supernode KCP 包

收到 KCP 包后：

- 先读 `conv`
- 如果命中 `ctrl_conv`，喂给 `sn_kcp_ctrl`
- 如果命中 `data_conv`，喂给 `sn_kcp_data`
- 否则按异常包丢弃

### supernode 收 edge KCP 包

建议：

- 先按 `conv` 在 `ctrl` 表查
- 再按 `conv` 在 `data` 表查
- 若两边都没有，再根据“这是哪条会话的点火包”创建对应 ctx

因此第一版里，conv 本身就是最关键的 channel 标识。

## 会话建立策略

建议第一版采用：

- 启动/注册阶段只主动点火 `ctrl KCP`
- 第一次真正发送 `n2n_packet` 时才懒建立 `data KCP`

优点：

- 控制面先稳定
- 无业务时不维护空的数据会话
- 更易排查 ctrl/data 具体是哪条坏了

## fallback 语义建议

第一版建议简单定义为：

- supernode 控制面是否失联，只看 `ctrl KCP`
- `data KCP` 坏了，不直接触发 TCP fallback
- `data KCP` 坏了，只影响业务数据，后续业务可重新懒建

这样可以让 supernode 控制判断更接近真实控制面状态。

## 预期收益

做完这一步但还没做内层 TCP/UDP 分流时，主要收益是：

- `REGISTER_SUPER_ACK`、`PEER_INFO` 不容易被业务大流量堵住
- `wrong or old cookie` 明显减少
- `deferring TCP fallback...` 更可信，因为它反映的是控制面 KCP 活性
- 即使业务数据面抖动，supernode 控制面也更稳定

## 不能解决的问题

这一步不能彻底解决：

- 内层 `TCP over KCP` 不适合的问题

它解决的是：

- 控制面与业务面共用一个 KCP 队列导致的相互污染

## 建议顺序

建议作为三步方案中的第二步：

1. 先调 KCP 参数
2. 再做双 KCP 会话
3. 最后做内层 TCP/UDP 分流
