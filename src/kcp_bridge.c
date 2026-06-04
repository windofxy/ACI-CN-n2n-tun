#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "n2n.h"
#include "n2n_wire.h"
#include "portable_endian.h"
#include "random_numbers.h"
#include "uthash.h"

#define N2N_KCP_INTERVAL_MS 10
#define N2N_KCP_SNDBUF_WND 256
#define N2N_KCP_RCVBUF_WND 256
#define N2N_KCP_DEFAULT_MTU 1200
#define N2N_KCP_OVERHEAD 24
#define N2N_KCP_CMD_PUSH 81u
#define N2N_KCP_CMD_ACK  82u
#define N2N_KCP_CMD_WASK 83u
#define N2N_KCP_CMD_WINS 84u
typedef struct n2n_kcp_edge_output_ctx {
    n2n_edge_t *eee;
    n2n_sock_t remote;
} n2n_kcp_edge_output_ctx_t;

typedef struct n2n_kcp_sn_output_ctx {
    SOCKET      socket_fd;
    socklen_t   sock_len;
    union {
        struct sockaddr         sock;
        struct sockaddr_storage sas;
    } addr;
} n2n_kcp_sn_output_ctx_t;

static IUINT32 n2n_kcp_max_segment_xmit (const ikcpcb *kcp) {
    const struct IQUEUEHEAD *p;
    IUINT32 max_xmit = 0;

    if(!kcp)
        return 0;

    for(p = kcp->snd_buf.next; p != &kcp->snd_buf; p = p->next) {
        const struct IKCPSEG *seg = iqueue_entry(p, const struct IKCPSEG, node);
        if(seg->xmit > max_xmit)
            max_xmit = seg->xmit;
    }

    return max_xmit;
}


static void n2n_kcp_apply_default_timeout_congestion (ikcpcb *kcp, IUINT32 prior_cwnd) {
    if(!kcp)
        return;

    kcp->ssthresh = prior_cwnd / 2;
    if(kcp->ssthresh < 2)
        kcp->ssthresh = 2;
    kcp->cwnd = 1;
    kcp->incr = kcp->mss;
}


static void n2n_kcp_edge_on_timeout (ikcpcb *kcp, IUINT32 prior_cwnd) {
    n2n_kcp_edge_output_ctx_t *ctx = (n2n_kcp_edge_output_ctx_t*)kcp->user;
    n2n_sock_str_t sockbuf;
    IUINT32 max_xmit = n2n_kcp_max_segment_xmit(kcp);
    IUINT32 remaining_before_dead = (kcp->dead_link > max_xmit) ? (kcp->dead_link - max_xmit) : 0;

    if(remaining_before_dead == 0) {
        traceEvent(TRACE_NORMAL,
                   "KCP session to supernode [%s] reached dead_link threshold (rx_rto=%u ms)",
                   sock_to_cstr(sockbuf, &ctx->remote),
                   (unsigned int)kcp->rx_rto);
    }

    n2n_kcp_apply_default_timeout_congestion(kcp, prior_cwnd);
}


static void n2n_kcp_sn_on_timeout (ikcpcb *kcp, IUINT32 prior_cwnd) {
    n2n_kcp_sn_output_ctx_t *ctx = (n2n_kcp_sn_output_ctx_t*)kcp->user;
    n2n_sock_t remote;
    n2n_sock_str_t sockbuf;
    IUINT32 max_xmit = n2n_kcp_max_segment_xmit(kcp);
    IUINT32 remaining_before_dead = (kcp->dead_link > max_xmit) ? (kcp->dead_link - max_xmit) : 0;

    fill_n2nsock(&remote, &ctx->addr.sock);

    if(remaining_before_dead == 0) {
        traceEvent(TRACE_NORMAL,
                   "KCP session to edge [%s] reached dead_link threshold (rx_rto=%u ms)",
                   sock_to_cstr(sockbuf, &remote),
                   (unsigned int)kcp->rx_rto);
    }

    n2n_kcp_apply_default_timeout_congestion(kcp, prior_cwnd);
}


static const struct IKCPOPS n2n_kcp_edge_ccops = {
    "n2n-edge-kcp",
    NULL,
    NULL,
    NULL,
    NULL,
    n2n_kcp_edge_on_timeout,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};


static const struct IKCPOPS n2n_kcp_sn_ccops = {
    "n2n-sn-kcp",
    NULL,
    NULL,
    NULL,
    NULL,
    n2n_kcp_sn_on_timeout,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};

static int n2n_kcp_raw_sendto (SOCKET fd, const uint8_t *buf, size_t len, const n2n_sock_t *dest) {
    struct sockaddr_in peer_addr;
    fill_sockaddr((struct sockaddr*)&peer_addr, sizeof(peer_addr), dest);
    return (int)sendto(fd, (const char*)buf, (int)len, 0, (struct sockaddr*)&peer_addr, sizeof(peer_addr));
}

static int n2n_kcp_recv_pending_ctx (n2n_kcp_ctx_t *ctx, uint8_t *out_buf, size_t out_buf_size, ssize_t *out_len) {
    int peeksize;
    int recv_len;

    if(out_len) *out_len = 0;
    if(!ctx || !ctx->active || !ctx->kcp || !out_buf || !out_len)
        return 0;

    peeksize = ikcp_peeksize(ctx->kcp);
    if(peeksize <= 0 || (size_t)peeksize > out_buf_size)
        return 0;

    recv_len = ikcp_recv(ctx->kcp, (char*)out_buf, peeksize);
    if(recv_len < 0)
        return 0;

    *out_len = recv_len;
    return 1;
}

static int n2n_kcp_packet_matches_conv (const uint8_t *buf, size_t len, uint32_t conv) {
    if(!buf || len < (size_t)N2N_KCP_OVERHEAD)
        return 0;

    return ikcp_getconv(buf) == conv;
}

static uint32_t n2n_kcp_read_le32 (const uint8_t *p) {
    return ((uint32_t)p[0])
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

static int n2n_kcp_cmd_is_valid (uint8_t cmd) {
    return (cmd == N2N_KCP_CMD_PUSH)
        || (cmd == N2N_KCP_CMD_ACK)
        || (cmd == N2N_KCP_CMD_WASK)
        || (cmd == N2N_KCP_CMD_WINS);
}

static int n2n_kcp_packet_get_conv (const uint8_t *buf, size_t len, uint32_t *conv) {
    const uint8_t *p = buf;
    size_t remaining = len;
    uint32_t packet_conv;

    if(conv)
        *conv = 0;

    if(!buf || !conv || len < (size_t)N2N_KCP_OVERHEAD)
        return 0;

    packet_conv = ikcp_getconv(buf);

    while(remaining >= (size_t)N2N_KCP_OVERHEAD) {
        uint32_t seg_conv = n2n_kcp_read_le32(p);
        uint8_t cmd = p[4];
        uint32_t seg_len = n2n_kcp_read_le32(p + 20);

        if(seg_conv != packet_conv)
            return 0;

        if(!n2n_kcp_cmd_is_valid(cmd))
            return 0;

        if(seg_len > (remaining - (size_t)N2N_KCP_OVERHEAD))
            return 0;

        p += N2N_KCP_OVERHEAD + seg_len;
        remaining -= N2N_KCP_OVERHEAD + seg_len;
    }

    if(remaining != 0)
        return 0;

    *conv = packet_conv;
    return 1;
}

static int n2n_kcp_wait_timeout_ms_ctx (const n2n_kcp_ctx_t *ctx, uint32_t current_ms, int default_ms) {
    uint32_t next_update;
    uint32_t delta;

    if(!ctx || !ctx->active || !ctx->kcp)
        return default_ms;

    next_update = ikcp_check(ctx->kcp, current_ms);
    delta = (next_update <= current_ms) ? 0u : (next_update - current_ms);

    if(delta > (uint32_t)default_ms)
        return default_ms;

    return (int)delta;
}

uint32_t n2n_kcp_now_ms (void) {
    struct timeval tv;
    uint64_t now_ms;

    gettimeofday(&tv, NULL);
    now_ms = ((uint64_t)tv.tv_sec * 1000u) + ((uint64_t)tv.tv_usec / 1000u);

    return (uint32_t)(now_ms & 0xFFFFFFFFu);
}

static uint32_t n2n_kcp_generate_conv (void) {
    uint32_t conv = (uint32_t)(n2n_rand() & 0xFFFFFFFFu);

    if(conv == 0)
        conv = 1;

    return conv;
}

void n2n_kcp_ctx_init (n2n_kcp_ctx_t *ctx) {
    if(ctx) memset(ctx, 0, sizeof(*ctx));
}

void n2n_kcp_ctx_term (n2n_kcp_ctx_t *ctx) {
    if(!ctx) return;
    if(ctx->kcp) {
        if(ctx->kcp->user) free(ctx->kcp->user);
        ikcp_release(ctx->kcp);
    }
    memset(ctx, 0, sizeof(*ctx));
}

static void n2n_kcp_configure (ikcpcb *kcp) {
    ikcp_nodelay(kcp, 1, N2N_KCP_INTERVAL_MS, 2, 1);
    ikcp_wndsize(kcp, N2N_KCP_SNDBUF_WND, N2N_KCP_RCVBUF_WND);
    ikcp_setmtu(kcp, N2N_KCP_DEFAULT_MTU);
    kcp->rx_minrto = 30;
    kcp->dead_link = 15;
}

static int n2n_kcp_edge_output (const char *buf, int len, ikcpcb *kcp, void *user) {
    n2n_kcp_edge_output_ctx_t *ctx = (n2n_kcp_edge_output_ctx_t*)user;
    (void)kcp;
    return n2n_kcp_raw_sendto(ctx->eee->udp_sock, (const uint8_t*)buf, (size_t)len, &ctx->remote);
}

static int n2n_kcp_sn_output (const char *buf, int len, ikcpcb *kcp, void *user) {
    n2n_kcp_sn_output_ctx_t *ctx = (n2n_kcp_sn_output_ctx_t*)user;
    (void)kcp;
    return (int)sendto(ctx->socket_fd, buf, len, 0, &ctx->addr.sock, ctx->sock_len);
}

static int n2n_kcp_edge_setup_with_conv (n2n_edge_t *eee, const n2n_sock_t *remote, uint32_t conv) {
    n2n_kcp_edge_output_ctx_t *user;
    n2n_sock_str_t sockbuf;
    n2n_sock_str_t prev_sockbuf;
    if(!eee || !remote || eee->udp_sock < 0) return 0;
    if(eee->sn_kcp.active && sock_equal(&eee->sn_kcp.remote_sock, remote)) return 0;
    if(eee->sn_kcp.active || eee->sn_kcp_confirmed) {
        traceEvent(TRACE_NORMAL,
                   "reinitializing edge KCP session from supernode [%s] to [%s] (old_family=%u old_port=%u new_family=%u new_port=%u)",
                   eee->sn_kcp.active ? sock_to_cstr(prev_sockbuf, &eee->sn_kcp.remote_sock) : "unbound",
                   sock_to_cstr(sockbuf, remote),
                   (unsigned int)eee->sn_kcp.remote_sock.family,
                   (unsigned int)eee->sn_kcp.remote_sock.port,
                   (unsigned int)remote->family,
                   (unsigned int)remote->port);
    }
    n2n_kcp_ctx_term(&eee->sn_kcp);
    eee->sn_kcp_confirmed = 0;
    user = (n2n_kcp_edge_output_ctx_t*)calloc(1, sizeof(*user));
    if(!user) return -1;
    user->eee = eee;
    memcpy(&user->remote, remote, sizeof(*remote));
    eee->sn_kcp.conv = conv;
    memcpy(&eee->sn_kcp.remote_sock, remote, sizeof(*remote));
    eee->sn_kcp.kcp = ikcp_create(eee->sn_kcp.conv, user);
    if(!eee->sn_kcp.kcp) {
        free(user);
        return -1;
    }
    eee->sn_kcp.kcp->output = n2n_kcp_edge_output;
    ikcp_setcc(eee->sn_kcp.kcp, &n2n_kcp_edge_ccops);
    n2n_kcp_configure(eee->sn_kcp.kcp);
    eee->sn_kcp.active = 1;
    eee->sn_kcp.rx_confirm_count = 0;
    eee->sn_kcp.last_seen = time(NULL);
    traceEvent(TRACE_DEBUG, "initialized KCP session to supernode [%s]",
               sock_to_cstr(sockbuf, remote));
    return 0;
}

int n2n_kcp_edge_setup (n2n_edge_t *eee, const n2n_sock_t *remote) {
    return n2n_kcp_edge_setup_with_conv(eee, remote, n2n_kcp_generate_conv());
}

int n2n_kcp_edge_send (n2n_edge_t *eee, const uint8_t *buf, size_t len, const n2n_sock_t *dest) {
    if(!eee || !buf || !dest || eee->udp_sock < 0) return -1;
    if(!sock_equal(dest, &eee->curr_sn->sock)) return -1;
    if(n2n_kcp_edge_setup(eee, dest) != 0) return -1;
    if(ikcp_send(eee->sn_kcp.kcp, (const char*)buf, (int)len) < 0) return -1;
    ikcp_update(eee->sn_kcp.kcp, n2n_kcp_now_ms());
    return (int)len;
}

int n2n_kcp_edge_input (n2n_edge_t *eee, const struct sockaddr *sender_sock, const uint8_t *buf, size_t len, time_t now, uint8_t *out_buf, size_t out_buf_size, ssize_t *out_len) {
    n2n_sock_t sender;
    uint32_t conv;
    n2n_sock_str_t sockbuf;
    int had_active_ctx;

    if(out_len) *out_len = 0;
    if(!eee || eee->udp_sock < 0 || !out_buf || !out_len) return 0;
    fill_n2nsock(&sender, sender_sock);
    if(!sock_equal(&sender, &eee->curr_sn->sock)) return 0;
    if(!n2n_kcp_packet_get_conv(buf, len, &conv)) return 0;
    if(!eee->sn_kcp.active || !eee->sn_kcp.kcp || !sock_equal(&eee->sn_kcp.remote_sock, &sender))
        return 0;
    if(conv != eee->sn_kcp.conv)
        return 0;
    traceEvent(TRACE_DEBUG,
               "received %u-byte KCP packet from supernode [%s]",
               (unsigned int)len,
               sock_to_cstr(sockbuf, &sender));
    had_active_ctx = (eee->sn_kcp.active && eee->sn_kcp.kcp && sock_equal(&eee->sn_kcp.remote_sock, &sender));
    if(ikcp_input(eee->sn_kcp.kcp, (const char*)buf, (long)len) < 0) {
        if(!had_active_ctx) {
            traceEvent(TRACE_WARNING,
                       "dropping freshly initialized KCP session to supernode [%s]: initial ikcp_input failed",
                       sock_to_cstr(sockbuf, &sender));
            n2n_kcp_ctx_term(&eee->sn_kcp);
            eee->sn_kcp_confirmed = 0;
        }
        return 0;
    }
    if(!eee->sn_kcp_confirmed && eee->sn_kcp.rx_confirm_count < 0xFF)
        eee->sn_kcp.rx_confirm_count++;
    if(!eee->sn_kcp_confirmed && eee->sn_kcp.rx_confirm_count >= 2) {
        eee->sn_kcp_confirmed = 1;
        traceEvent(TRACE_NORMAL, "KCP session to supernode [%s] established",
                   sock_to_cstr(sockbuf, &sender));
    } else if(!eee->sn_kcp_confirmed) {
        traceEvent(TRACE_DEBUG,
                   "KCP session to supernode [%s] received warmup packet %u/2",
                   sock_to_cstr(sockbuf, &sender),
                   (unsigned int)eee->sn_kcp.rx_confirm_count);
    }
    eee->sn_kcp.last_seen = now;
    ikcp_update(eee->sn_kcp.kcp, n2n_kcp_now_ms());
    n2n_kcp_recv_pending_ctx(&eee->sn_kcp, out_buf, out_buf_size, out_len);
    return 1;
}

int n2n_kcp_edge_recv_pending (n2n_edge_t *eee, uint8_t *out_buf, size_t out_buf_size, ssize_t *out_len) {
    if(out_len) *out_len = 0;
    if(!eee)
        return 0;

    return n2n_kcp_recv_pending_ctx(&eee->sn_kcp, out_buf, out_buf_size, out_len);
}

void n2n_kcp_edge_update (n2n_edge_t *eee) {
    if(eee && eee->sn_kcp.active && eee->sn_kcp.kcp) {
        ikcp_update(eee->sn_kcp.kcp, n2n_kcp_now_ms());
    }
}

int n2n_kcp_edge_wait_timeout_ms (const n2n_edge_t *eee, int default_ms) {
    if(!eee)
        return default_ms;

    return n2n_kcp_wait_timeout_ms_ctx(&eee->sn_kcp, n2n_kcp_now_ms(), default_ms);
}

static void n2n_kcp_sn_drop_ctx (n2n_sn_t *sss, n2n_kcp_ctx_t *ctx, const char *reason) {
    n2n_sock_str_t sockbuf;

    if(!sss || !ctx)
        return;

    traceEvent(TRACE_WARNING,
               "dropping KCP session for edge [%s]: %s",
               sock_to_cstr(sockbuf, &ctx->remote_sock),
               reason ? reason : "unspecified");

    HASH_DEL(sss->udp_kcp_connections, ctx);
    n2n_kcp_ctx_term(ctx);
    free(ctx);
}

static n2n_kcp_ctx_t *n2n_kcp_sn_find_or_create (n2n_sn_t *sss, SOCKET socket_fd, const struct sockaddr *sender_sock, socklen_t sender_len, uint32_t conv, int *created) {
    n2n_kcp_ctx_t *ctx;
    n2n_sock_t remote;
    n2n_kcp_sn_output_ctx_t *user;
    n2n_sock_str_t sockbuf;

    if(created)
        *created = 0;

    fill_n2nsock(&remote, sender_sock);
    HASH_FIND(hh, sss->udp_kcp_connections, &remote, sizeof(n2n_sock_t), ctx);
    if(ctx) {
        if(ctx->kcp && ctx->kcp->state == (IUINT32)-1) {
            n2n_kcp_sn_drop_ctx(sss, ctx, "edge sent new traffic after KCP dead_link");
            ctx = NULL;
        } else if(ctx->conv != conv) {
            traceEvent(TRACE_NORMAL,
                       "reinitializing KCP session for edge [%s] because conv changed from %u to %u",
                       sock_to_cstr(sockbuf, &remote),
                       (unsigned int)ctx->conv,
                       (unsigned int)conv);
            n2n_kcp_sn_drop_ctx(sss, ctx, "edge restarted KCP session with new conv");
            ctx = NULL;
        } else {
            traceEvent(TRACE_DEBUG, "reusing KCP session for edge [%s]", sock_to_cstr(sockbuf, &remote));
            return ctx;
        }
    }
    ctx = (n2n_kcp_ctx_t*)calloc(1, sizeof(*ctx));
    if(!ctx) return NULL;
    memcpy(&ctx->remote_sock, &remote, sizeof(remote));
    ctx->conv = conv;
    user = (n2n_kcp_sn_output_ctx_t*)calloc(1, sizeof(*user));
    if(!user) {
        free(ctx);
        return NULL;
    }
    user->socket_fd = socket_fd;
    user->sock_len = sender_len;
    memcpy(&user->addr.sock, sender_sock, sender_len);
    ctx->kcp = ikcp_create(ctx->conv, user);
    if(!ctx->kcp) {
        free(user);
        free(ctx);
        return NULL;
    }
    ctx->kcp->output = n2n_kcp_sn_output;
    ikcp_setcc(ctx->kcp, &n2n_kcp_sn_ccops);
    n2n_kcp_configure(ctx->kcp);
    ctx->active = 1;
    ctx->last_seen = time(NULL);
    HASH_ADD(hh, sss->udp_kcp_connections, remote_sock, sizeof(n2n_sock_t), ctx);
    if(created)
        *created = 1;
    traceEvent(TRACE_DEBUG, "created KCP session for edge [%s]", sock_to_cstr(sockbuf, &remote));
    return ctx;
}

int n2n_kcp_sn_send (n2n_sn_t *sss, SOCKET socket_fd, const struct sockaddr *socket, const uint8_t *pktbuf, size_t pktsize) {
    n2n_kcp_ctx_t *ctx;
    socklen_t sock_len = sizeof(struct sockaddr_in);
    n2n_sock_t remote;
    n2n_sock_str_t sockbuf;
    if(!sss || !socket || socket_fd != sss->sock) return -1;
    fill_n2nsock(&remote, socket);
    HASH_FIND(hh, sss->udp_kcp_connections, &remote, sizeof(n2n_sock_t), ctx);
    if(!ctx) return -1;
    traceEvent(TRACE_DEBUG,
               "sending %u-byte payload to edge [%s] over KCP",
               (unsigned int)pktsize,
               sock_to_cstr(sockbuf, &remote));
    if(ikcp_send(ctx->kcp, (const char*)pktbuf, (int)pktsize) < 0) return -1;
    ikcp_update(ctx->kcp, n2n_kcp_now_ms());
    return (int)pktsize;
}

int n2n_kcp_sn_process_input (n2n_sn_t *sss, const struct sockaddr *sender_sock, socklen_t sender_len, const uint8_t *buf, size_t len, time_t now, uint8_t *out_buf, size_t out_buf_size, ssize_t *out_len) {
    n2n_kcp_ctx_t *ctx;
    n2n_sock_t remote;
    n2n_sock_str_t sockbuf;
    uint32_t conv;
    int rc;
    int created = 0;
    if(out_len) *out_len = 0;
    if(!sss || !sender_sock || !buf || !out_buf || !out_len) return 0;
    fill_n2nsock(&remote, sender_sock);
    if(!n2n_kcp_packet_get_conv(buf, len, &conv)) return 0;
    traceEvent(TRACE_DEBUG,
               "received %u-byte KCP packet from edge [%s]",
               (unsigned int)len,
               sock_to_cstr(sockbuf, &remote));
    ctx = n2n_kcp_sn_find_or_create(sss, sss->sock, sender_sock, sender_len, conv, &created);
    if(!ctx) return 0;
    rc = ikcp_input(ctx->kcp, (const char*)buf, (long)len);
    if(rc < 0) {
        if(created) {
            n2n_kcp_sn_drop_ctx(sss, ctx, "initial ikcp_input failed");
        }
        return 0;
    }
    ctx->last_seen = now;
    ikcp_update(ctx->kcp, n2n_kcp_now_ms());
    n2n_kcp_recv_pending_ctx(ctx, out_buf, out_buf_size, out_len);
    return 1;
}

int n2n_kcp_sn_recv_pending (n2n_sn_t *sss, const struct sockaddr *sender_sock, socklen_t sender_len, uint8_t *out_buf, size_t out_buf_size, ssize_t *out_len) {
    n2n_kcp_ctx_t *ctx;
    n2n_sock_t remote;

    if(out_len) *out_len = 0;
    if(!sss || !sender_sock)
        return 0;

    fill_n2nsock(&remote, sender_sock);
    HASH_FIND(hh, sss->udp_kcp_connections, &remote, sizeof(n2n_sock_t), ctx);
    if(!ctx)
        return 0;

    (void)sender_len;
    return n2n_kcp_recv_pending_ctx(ctx, out_buf, out_buf_size, out_len);
}

void n2n_kcp_sn_update (n2n_sn_t *sss) {
    n2n_kcp_ctx_t *ctx, *tmp;
    uint32_t now_ms = n2n_kcp_now_ms();
    time_t now = time(NULL);
    if(!sss) return;
    HASH_ITER(hh, sss->udp_kcp_connections, ctx, tmp) {
        if(ctx->kcp) ikcp_update(ctx->kcp, now_ms);
        if(ctx->kcp && ctx->kcp->state == (IUINT32)-1) {
            n2n_kcp_sn_drop_ctx(sss, ctx, "KCP session reached dead_link threshold");
            continue;
        }
        if((now - ctx->last_seen) > 120) {
            n2n_kcp_sn_drop_ctx(sss, ctx, "inactive for 120 seconds");
        }
    }
}

int n2n_kcp_sn_wait_timeout_ms (const n2n_sn_t *sss, int default_ms) {
    const n2n_kcp_ctx_t *ctx;
    const n2n_kcp_ctx_t *tmp;
    int wait_ms = default_ms;
    uint32_t current_ms = n2n_kcp_now_ms();

    if(!sss)
        return default_ms;

    HASH_ITER(hh, sss->udp_kcp_connections, ctx, tmp) {
        int ctx_wait_ms = n2n_kcp_wait_timeout_ms_ctx(ctx, current_ms, default_ms);
        if(ctx_wait_ms < wait_ms)
            wait_ms = ctx_wait_ms;
    }

    return wait_ms;
}
