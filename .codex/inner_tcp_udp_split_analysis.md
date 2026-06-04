# 内层 TCP/UDP 分流分析

## 目标

让业务 `n2n_packet` 按内层协议类型选择外层传输：

- 内层 TCP：走纯 UDP
- 内层 UDP：走 KCP

核心目的不是“增强可靠性”，而是避免内层 TCP 再被外层 KCP 的可靠有序特性拖坏。

## 当前问题

当前实现中：

- `edge` / `supernode` 基本按整帧透明转发 `n2n_packet`
- 正常转发逻辑不会按内层是 TCP 还是 UDP 做不同策略
- 一旦 supernode 路径进入 KCP，内层 TCP 与内层 UDP 都可能一起进入同一条 KCP 通道

这会带来典型的 `TCP over KCP` 问题：

- 头阻塞
- 重传等待
- 队列积压
- 恢复后突刺
- 应用层超时或 RST

## 关键困难

真正的问题不是源 edge 看不出内层协议，而是：

- supernode 当前看不到“这个业务包应该走 UDP 还是 KCP”的判断结果

所以必须把源 edge 的判断结果变成 supernode 可见的外层元信息。

## 最小协议思路

### 推荐方案

给 `n2n_PACKET_t` 增加一个很小的 transport hint，或复用保留 flag 位，表达：

- `AUTO`
- `INNER_TCP`
- `INNER_UDP`

语义定义：

- `INNER_TCP`：supernode 转给目标 edge 时强制 raw UDP
- `INNER_UDP`：supernode 转给目标 edge 时优先 KCP
- `AUTO`：维持旧逻辑

### 为什么要这样

这样可以让：

- 源 edge 负责识别内层协议
- supernode 负责执行具体的外层 transport 决策

而不需要让 supernode 解业务 payload 再自己判断内层是 TCP 还是 UDP。

## 源 edge 的判断位置

最适合加入判断的点：

- `src/edge_utils.c` 中从 TAP/Wintun 收到以太帧并构造 `n2n_PACKET_t` 的路径

第一版建议只做最小判断：

- IPv4 TCP -> `INNER_TCP`
- IPv4 UDP -> `INNER_UDP`
- 其他 -> `AUTO`
- IPv6 -> 第一版先 `AUTO`

这样可以避免一开始就处理 IPv6 extension headers 的复杂度。

## supernode 的执行位置

最适合执行 transport 分流的点：

- `src/sn_utils.c` 的 `try_forward()`

这里当前对业务包的处理是：

- 找到目标 peer
- 调 `sendto_peer()`
- 若有活动 KCP ctx 则优先走 KCP

改造后应变成：

- `n2n_packet + INNER_TCP` -> 强制 raw UDP
- `n2n_packet + INNER_UDP` -> 优先 KCP
- `AUTO` -> 保持旧逻辑

## sendto_peer 层的建议

当前 `sendto_peer()` 只有一个和业务相关的布尔参数：

- `drop_oversized_business_packet`

如果做内层协议分流，建议逐步演进为更明确的业务 transport policy：

- `DEFAULT`
- `FORCE_RAW_UDP`
- `PREFER_KCP`

这样：

- 控制面仍然使用自己的 channel 逻辑
- 业务包根据外层 hint 决定 policy

后续若要扩展端口/业务类型特判，也更容易继续扩展。

## 兼容性考虑

### 新 edge -> 旧 supernode

- 新 edge 能生成 hint
- 旧 supernode 若忽略 hint，则只会失去分流能力
- 不应导致协议直接不兼容

### 旧 edge -> 新 supernode

- 旧 edge 不带 hint
- 新 supernode 看到 `AUTO`
- 继续走旧逻辑

### 新 edge -> 新 supernode

- 才能完整启用分流策略

因此 hint 最好放在：

- 老版本能忽略的位置

而不要直接破坏旧版 `decode_PACKET()` 的固定布局。

## 预期收益

如果分流正确实现：

- 内层 TCP
  - 不再被 KCP 头阻塞和恢复突刺影响
  - 应用层 timeout / RST 概率通常会下降
- 内层 UDP
  - 仍可利用 KCP 降低丢包影响

对当前问题最直接的改善，通常会体现在：

- RPCN 类应用层 TCP 错误减少
- 游戏数据转发器中的 TCP 连接超时减少

## 不能保证的事情

即使做了分流：

- raw UDP 路径仍可能真实丢包
- 底层公网质量差时，内层 TCP 仍可能慢

它解决的是：

- 不再让内层 TCP 额外承受外层 KCP 的可靠有序语义

而不是让 TCP 一定“绝不超时”。

## 建议顺序

建议作为三步方案中的第三步：

1. 先调 KCP 参数
2. 再做双 KCP 会话
3. 最后做内层 TCP/UDP 分流

这样前两步先稳定控制面与 KCP 队列行为，第三步再针对内层 TCP 问题做真正对症处理。
