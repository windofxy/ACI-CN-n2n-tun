#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "n2n.h"
#include "n2n_wire.h"
#include "portable_endian.h"
#include "uthash.h"

#define N2N_KCP_INTERVAL_MS 10
#define N2N_KCP_SNDBUF_WND 128
#define N2N_KCP_RCVBUF_WND 128
#define N2N_KCP_DEFAULT_MTU 1200
#define N2N_KCP_FIXED_CONV 0x4e324e31u

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

static int n2n_kcp_raw_sendto (SOCKET fd, const uint8_t *buf, size_t len, const n2n_sock_t *dest) {
    struct sockaddr_in peer_addr;
    fill_sockaddr((struct sockaddr*)&peer_addr, sizeof(peer_addr), dest);
    return (int)sendto(fd, (const char*)buf, (int)len, 0, (struct sockaddr*)&peer_addr, sizeof(peer_addr));
}

uint32_t n2n_kcp_now_ms (void) {
    return (uint32_t)(time_stamp() >> 12);
}

uint32_t n2n_kcp_conv_for_sock (const n2n_sock_t *sock) {
    (void)sock;
    return N2N_KCP_FIXED_CONV;
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
    kcp->rx_minrto = 10;
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

int n2n_kcp_edge_setup (n2n_edge_t *eee, const n2n_sock_t *remote) {
    n2n_kcp_edge_output_ctx_t *user;
    if(!eee || !remote || eee->udp_sock < 0) return 0;
    if(eee->sn_kcp.active && sock_equal(&eee->sn_kcp.remote_sock, remote)) return 0;
    n2n_kcp_ctx_term(&eee->sn_kcp);
    user = (n2n_kcp_edge_output_ctx_t*)calloc(1, sizeof(*user));
    if(!user) return -1;
    user->eee = eee;
    memcpy(&user->remote, remote, sizeof(*remote));
    eee->sn_kcp.conv = n2n_kcp_conv_for_sock(remote);
    memcpy(&eee->sn_kcp.remote_sock, remote, sizeof(*remote));
    eee->sn_kcp.kcp = ikcp_create(eee->sn_kcp.conv, user);
    if(!eee->sn_kcp.kcp) {
        free(user);
        return -1;
    }
    eee->sn_kcp.kcp->output = n2n_kcp_edge_output;
    n2n_kcp_configure(eee->sn_kcp.kcp);
    eee->sn_kcp.active = 1;
    eee->sn_kcp.last_seen = time(NULL);
    return 0;
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
    if(out_len) *out_len = 0;
    if(!eee || eee->udp_sock < 0 || !out_buf || !out_len) return 0;
    fill_n2nsock(&sender, sender_sock);
    if(!sock_equal(&sender, &eee->curr_sn->sock)) return 0;
    if(n2n_kcp_edge_setup(eee, &sender) != 0) return 0;
    if(ikcp_input(eee->sn_kcp.kcp, (const char*)buf, (long)len) < 0) return 0;
    eee->sn_kcp.last_seen = now;
    ikcp_update(eee->sn_kcp.kcp, n2n_kcp_now_ms());
    *out_len = ikcp_recv(eee->sn_kcp.kcp, (char*)out_buf, (int)out_buf_size);
    if(*out_len < 0) *out_len = 0;
    return 1;
}

void n2n_kcp_edge_update (n2n_edge_t *eee) {
    if(eee && eee->sn_kcp.active && eee->sn_kcp.kcp) {
        ikcp_update(eee->sn_kcp.kcp, n2n_kcp_now_ms());
    }
}

static n2n_kcp_ctx_t *n2n_kcp_sn_find_or_create (n2n_sn_t *sss, SOCKET socket_fd, const struct sockaddr *sender_sock, socklen_t sender_len) {
    n2n_kcp_ctx_t *ctx;
    n2n_sock_t remote;
    n2n_kcp_sn_output_ctx_t *user;
    fill_n2nsock(&remote, sender_sock);
    HASH_FIND(hh, sss->udp_kcp_connections, &remote, sizeof(n2n_sock_t), ctx);
    if(ctx) return ctx;
    ctx = (n2n_kcp_ctx_t*)calloc(1, sizeof(*ctx));
    if(!ctx) return NULL;
    memcpy(&ctx->remote_sock, &remote, sizeof(remote));
    ctx->conv = n2n_kcp_conv_for_sock(&remote);
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
    n2n_kcp_configure(ctx->kcp);
    ctx->active = 1;
    ctx->last_seen = time(NULL);
    HASH_ADD(hh, sss->udp_kcp_connections, remote_sock, sizeof(n2n_sock_t), ctx);
    return ctx;
}

int n2n_kcp_sn_send (n2n_sn_t *sss, SOCKET socket_fd, const struct sockaddr *socket, const uint8_t *pktbuf, size_t pktsize) {
    n2n_kcp_ctx_t *ctx;
    socklen_t sock_len = sizeof(struct sockaddr_in);
    if(!sss || !socket || socket_fd != sss->sock) return -1;
    ctx = n2n_kcp_sn_find_or_create(sss, socket_fd, socket, sock_len);
    if(!ctx) return -1;
    if(ikcp_send(ctx->kcp, (const char*)pktbuf, (int)pktsize) < 0) return -1;
    ikcp_update(ctx->kcp, n2n_kcp_now_ms());
    return (int)pktsize;
}

int n2n_kcp_sn_process_input (n2n_sn_t *sss, const struct sockaddr *sender_sock, socklen_t sender_len, const uint8_t *buf, size_t len, time_t now, uint8_t *out_buf, size_t out_buf_size, ssize_t *out_len) {
    n2n_kcp_ctx_t *ctx;
    int rc;
    if(out_len) *out_len = 0;
    if(!sss || !sender_sock || !buf || !out_buf || !out_len) return 0;
    ctx = n2n_kcp_sn_find_or_create(sss, sss->sock, sender_sock, sender_len);
    if(!ctx) return 0;
    rc = ikcp_input(ctx->kcp, (const char*)buf, (long)len);
    if(rc < 0) return 0;
    ctx->last_seen = now;
    ikcp_update(ctx->kcp, n2n_kcp_now_ms());
    *out_len = ikcp_recv(ctx->kcp, (char*)out_buf, (int)out_buf_size);
    if(*out_len < 0) *out_len = 0;
    return 1;
}

void n2n_kcp_sn_update (n2n_sn_t *sss) {
    n2n_kcp_ctx_t *ctx, *tmp;
    uint32_t now_ms = n2n_kcp_now_ms();
    time_t now = time(NULL);
    if(!sss) return;
    HASH_ITER(hh, sss->udp_kcp_connections, ctx, tmp) {
        if(ctx->kcp) ikcp_update(ctx->kcp, now_ms);
        if((now - ctx->last_seen) > 120) {
            HASH_DEL(sss->udp_kcp_connections, ctx);
            n2n_kcp_ctx_term(ctx);
            free(ctx);
        }
    }
}
