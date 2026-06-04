# Supernode 传输模式对照

## 概览

当前分支里，`-S` 系列参数主要影响这 3 个开关：

- `connect_tcp`
- `prefer_kcp`
- `allow_tcp_fallback`

对应代码在 `src/edge.c` 的 `case 'S'`。

## 对照表

| 模式 | P2P | edge -> supernode 基础传输 | 控制面 | 业务内层 TCP | 业务内层 UDP / 广播 | 断线后行为 |
| --- | --- | --- | --- | --- | --- | --- |
| 默认 | 开 | UDP | 优先 control KCP | raw UDP | 优先 data KCP | UDP/KCP 不可用时回退 TCP，之后可恢复回 UDP |
| `-S1` | 关 | UDP | 优先 control KCP | raw UDP | 优先 data KCP | 只在 UDP/KCP 上重建，不回退 TCP |
| `-S2` | 关 | TCP | TCP | TCP | TCP | 只在 TCP 上重建，不尝试 UDP/KCP |
| `-S3` | 关 | UDP | 优先 control KCP | raw UDP | 优先 data KCP | UDP/KCP 不可用时回退 TCP，并在 TCP 断开后重新尝试 UDP/KCP |

## 每种模式的实际链路

### 默认

- `connect_tcp = 0`
- `prefer_kcp = 1`
- `allow_tcp_fallback = 1`
- 控制面先走 UDP，control KCP 建立后改走 control KCP
- 业务面默认优先走 data KCP
- 但内层 `TCP` 现在会被分流为 raw UDP，不再进入 data KCP
- 如果 UDP/KCP 到 supernode 不可用，会切到 TCP fallback

### `-S1`

- `connect_tcp = 0`
- `prefer_kcp = 1`
- `allow_tcp_fallback = 0`
- 效果和默认的 UDP/KCP 主路径一致
- 区别是禁用 TCP fallback
- 所以 KCP dead_link 或 UDP 不通时，会继续在 UDP/KCP 侧重建

### `-S2`

- `connect_tcp = 1`
- `prefer_kcp = 0`
- `allow_tcp_fallback = 0`
- edge 与 supernode 之间整条腿固定走 TCP
- 不会建立 supernode control/data KCP 会话
- 内层 TCP/UDP 分流逻辑对这条腿基本不生效，因为发送路径在更前面就走 TCP 了
- 如果另一端 edge 不是 `-S2`，那么 supernode 到另一端的腿仍按另一端自己的模式决定

### `-S3`

- `connect_tcp = 0`
- `prefer_kcp = 1`
- `allow_tcp_fallback = 1`
- 和默认类似，但它属于显式 supernode-only 模式
- 主路径仍然是 UDP + KCP
- UDP/KCP 断开时会切到 TCP
- TCP 断开后，会重新尝试回到 UDP/KCP

## 当前分支下的“业务分流”结论

在默认、`-S1`、`-S3` 下：

- `edge -> supernode`
  - 内层 `TCP` 走 raw UDP
  - 内层 `UDP` 优先走 data KCP
- `supernode -> edge`
  - 内层 `TCP` 走 raw UDP
  - 内层 `UDP` 优先走 data KCP

也就是说，当前已经实现了“内层 TCP 整条 supernode 路径不碰 KCP”。

在 `-S2` 下：

- `edge -> supernode` 直接 TCP
- `supernode -> edge` 如果目标 edge 也是 `-S2`，则也是 TCP
- 所以 `-S2` 本质上是“这条 edge-supernode 腿强制 TCP”

## 风险和预期

### 默认 / `-S1` / `-S3`

- 优点是控制面和 UDP 业务还能享受 KCP 的可靠性
- 内层 TCP 已避开 KCP，减少“TCP over KCP”带来的状态干扰
- 风险主要在 UDP 质量差时，raw UDP 承载的内层 TCP 会直接暴露丢包

### `-S2`

- 优点是路径最简单，完全不依赖 UDP/KCP
- 风险是所有业务都压到 supernode TCP 上
- 对应用层 TCP 来说，相当于“TCP over TCP”，更容易出现队头阻塞和抖动放大

## 关键代码位置

- `src/edge.c`
  - `case 'S'` 参数语义
- `src/edge_utils.c`
  - `edge_transport_is_forced_tcp()`
  - `edge_switch_to_tcp_supernode()`
  - `sendto_fd()`
  - `send_packet()`
  - `edge_packet_transport_flags()`
- `src/sn_utils.c`
  - `packet_transport_policy_from_flags()`
  - `sendto_fd()`

## 一句话总结

- 想要“KCP 负责控制面和 UDP 业务，TCP 业务不要碰 KCP”，用默认 / `-S1` / `-S3`
- 想要“完全不用 UDP/KCP，只走 supernode TCP”，用 `-S2`
