/*
 * Copyright (c) 2025 efogdev
 * SPDX-License-Identifier: MIT
 */

#include <zephyr/kernel.h>
#include <zephyr/init.h>
#include <zephyr/shell/shell.h>
#include <zephyr/sys/ring_buffer.h>
#include <string.h>
#include <errno.h>
#include <zephyr/logging/log.h>
#include <zmk_shell_relay/relay.h>

LOG_MODULE_REGISTER(zmk_shell_relay, CONFIG_ZMK_LOG_LEVEL);

struct zsr_transport_ctx {
    bool initialized;
};

static int zsr_tp_init(const struct shell_transport *transport, const void *config,
                       const shell_transport_handler_t evt_handler, void *context) {
    ARG_UNUSED(config);
    ARG_UNUSED(evt_handler);
    ARG_UNUSED(context);
    struct zsr_transport_ctx *ctx = (struct zsr_transport_ctx *)transport->ctx;
    if (ctx->initialized) {
        return -EINVAL;
    }
    ctx->initialized = true;
    return 0;
}

static int zsr_tp_uninit(const struct shell_transport *transport) {
    struct zsr_transport_ctx *ctx = (struct zsr_transport_ctx *)transport->ctx;
    if (!ctx->initialized) {
        return -ENODEV;
    }
    ctx->initialized = false;
    return 0;
}

static int zsr_tp_enable(const struct shell_transport *transport, const bool blocking) {
    ARG_UNUSED(blocking);
    const struct zsr_transport_ctx *ctx = (struct zsr_transport_ctx *)transport->ctx;
    return ctx->initialized ? 0 : -ENODEV;
}

static int zsr_tp_write(const struct shell_transport *transport, const void *data, const size_t length, size_t *cnt) {
    const struct zsr_transport_ctx *ctx = (struct zsr_transport_ctx *)transport->ctx;
    if (!ctx->initialized) {
        *cnt = 0;
        return -ENODEV;
    }
    zmk_shell_relay_enqueue((const uint8_t *)data, length);
    *cnt = length;
    return 0;
}

static int zsr_tp_read(const struct shell_transport *transport, void *data, const size_t length, size_t *cnt) {
    ARG_UNUSED(transport);
    ARG_UNUSED(data);
    ARG_UNUSED(length);
    *cnt = 0;
    return 0;
}

static const struct shell_transport_api zsr_transport_api = {
    .init = zsr_tp_init,
    .uninit = zsr_tp_uninit,
    .enable = zsr_tp_enable,
    .write = zsr_tp_write,
    .read = zsr_tp_read,
};

static struct zsr_transport_ctx zsr_transport_ctx;
static struct shell_transport zsr_transport = {
    .api = &zsr_transport_api,
    .ctx = &zsr_transport_ctx,
};

SHELL_DEFINE(zsr_shell, CONFIG_ZMK_SHELL_RELAY_PROMPT, &zsr_transport,
             CONFIG_ZMK_SHELL_RELAY_LOG_QUEUE_SIZE, 0, SHELL_FLAG_OLF_CRLF);
RING_BUF_DECLARE(zsr_static_rb, CONFIG_ZMK_SHELL_RELAY_STATIC_BUF_SIZE);

struct zsr_seg {
    struct zsr_seg *next;
    uint16_t cap;
    uint16_t len;
    uint16_t rd;
    uint8_t data[];
};

K_HEAP_DEFINE(zsr_overflow_heap, CONFIG_ZMK_SHELL_RELAY_OVERFLOW_HEAP_SIZE);

static struct k_spinlock zsr_lock;
static struct zsr_seg *zsr_ovf_head;
static struct zsr_seg *zsr_ovf_tail;
static uint32_t zsr_ovf_bytes;
static bool zsr_claim_static;
static const struct zmk_shell_relay_sink *zsr_sink;

static struct zsr_seg *zsr_seg_alloc(void) {
    struct zsr_seg *seg = k_heap_alloc(&zsr_overflow_heap, sizeof(struct zsr_seg) + CONFIG_ZMK_SHELL_RELAY_SEG_SIZE, K_NO_WAIT);
    if (seg == NULL) {
        return NULL;
    }
    seg->next = NULL;
    seg->cap = CONFIG_ZMK_SHELL_RELAY_SEG_SIZE;
    seg->len = 0;
    seg->rd = 0;
    return seg;
}

static void zsr_ovf_put(const uint8_t *data, size_t len) {
    while (len > 0) {
        struct zsr_seg *seg = zsr_ovf_tail;
        if (seg == NULL || seg->len == seg->cap) {
            seg = zsr_seg_alloc();
            if (seg == NULL) {
                LOG_WRN("overflow heap full: dropped %zu bytes", len);
                return;
            }
            if (zsr_ovf_tail != NULL) {
                zsr_ovf_tail->next = seg;
            } else {
                zsr_ovf_head = seg;
            }
            zsr_ovf_tail = seg;
        }
        const uint16_t space = seg->cap - seg->len;
        const size_t n = MIN(len, (size_t)space);
        memcpy(seg->data + seg->len, data, n);
        seg->len += (uint16_t)n;
        zsr_ovf_bytes += (uint32_t)n;
        data += n;
        len -= n;
    }
}

void zmk_shell_relay_enqueue(const uint8_t *data, const size_t len) {
    if (len == 0) {
        return;
    }

    const k_spinlock_key_t key = k_spin_lock(&zsr_lock);
    size_t remaining = len;
    if (zsr_ovf_head == NULL) {
        const uint32_t put = ring_buf_put(&zsr_static_rb, data, (uint32_t)len);
        data += put;
        remaining -= put;
    }
    if (remaining > 0) {
        zsr_ovf_put(data, remaining);
    }

    k_spin_unlock(&zsr_lock, key);

    if (zsr_sink != NULL && zsr_sink->data_ready != NULL) {
        zsr_sink->data_ready();
    }
}

uint32_t zmk_shell_relay_claim(uint8_t **data, const uint32_t max_len) {
    const k_spinlock_key_t key = k_spin_lock(&zsr_lock);

    uint32_t got = ring_buf_get_claim(&zsr_static_rb, data, max_len);
    if (got > 0) {
        zsr_claim_static = true;
        k_spin_unlock(&zsr_lock, key);
        return got;
    }

    if (zsr_ovf_head != NULL) {
        struct zsr_seg *seg = zsr_ovf_head;
        const uint32_t avail = seg->len - seg->rd;
        got = MIN(avail, max_len);
        *data = seg->data + seg->rd;
        zsr_claim_static = false;
        k_spin_unlock(&zsr_lock, key);
        return got;
    }

    *data = NULL;
    k_spin_unlock(&zsr_lock, key);
    return 0;
}

void zmk_shell_relay_finish(const uint32_t consumed) {
    const k_spinlock_key_t key = k_spin_lock(&zsr_lock);

    if (zsr_claim_static) {
        ring_buf_get_finish(&zsr_static_rb, consumed);
        k_spin_unlock(&zsr_lock, key);
        return;
    }

    struct zsr_seg *seg = zsr_ovf_head;
    if (seg != NULL) {
        seg->rd += (uint16_t)consumed;
        zsr_ovf_bytes -= consumed;
        if (seg->rd >= seg->len) {
            zsr_ovf_head = seg->next;
            if (zsr_ovf_head == NULL) {
                zsr_ovf_tail = NULL;
            }
            k_heap_free(&zsr_overflow_heap, seg);
        }
    }

    k_spin_unlock(&zsr_lock, key);
}

uint32_t zmk_shell_relay_size(void) {
    const k_spinlock_key_t key = k_spin_lock(&zsr_lock);
    const uint32_t sz = ring_buf_size_get(&zsr_static_rb) + zsr_ovf_bytes;
    k_spin_unlock(&zsr_lock, key);
    return sz;
}

void zmk_shell_relay_reset(void) {
    const k_spinlock_key_t key = k_spin_lock(&zsr_lock);
    ring_buf_reset(&zsr_static_rb);
    struct zsr_seg *seg = zsr_ovf_head;
    while (seg != NULL) {
        struct zsr_seg *next = seg->next;
        k_heap_free(&zsr_overflow_heap, seg);
        seg = next;
    }
    zsr_ovf_head = NULL;
    zsr_ovf_tail = NULL;
    zsr_ovf_bytes = 0;
    zsr_claim_static = false;
    k_spin_unlock(&zsr_lock, key);
}

void zmk_shell_relay_attach(const struct zmk_shell_relay_sink *sink) {
    zsr_sink = sink;
}

void zmk_shell_relay_detach(void) {
    zsr_sink = NULL;
}

static K_THREAD_STACK_DEFINE(zsr_exec_stack, CONFIG_ZMK_SHELL_RELAY_EXEC_STACK_SIZE);
static struct k_work_q zsr_exec_q;

RING_BUF_DECLARE(zsr_cmd_rb, CONFIG_ZMK_SHELL_RELAY_CMD_QUEUE_SIZE);
static struct k_spinlock zsr_cmd_lock;

static void zsr_exec_work_fn(struct k_work *work) {
    ARG_UNUSED(work);
    char cmd[CONFIG_ZMK_SHELL_RELAY_CMD_BUF_SIZE];

    while (true) {
        size_t n = 0;
        uint8_t c;
        while (ring_buf_get(&zsr_cmd_rb, &c, 1) == 1 && c != '\0') {
            if (n < sizeof(cmd) - 1) {
                cmd[n++] = (char)c;
            }
        }
        if (n == 0) {
            return;
        }
        cmd[n] = '\0';

        char *start = cmd;
        while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') {
            start++;
        }
        char *end = start + strlen(start);
        while (end > start && (*(end - 1) == ' ' || *(end - 1) == '\t' || *(end - 1) == '\r' || *(end - 1) == '\n')) {
            end--;
        }
        *end = '\0';
        if (*start == '\0') {
            continue;
        }

        const int ret = shell_execute_cmd(&zsr_shell, start);
        const struct zmk_shell_relay_sink *sink = zsr_sink;
        if (sink != NULL && sink->cmd_done != NULL) {
            sink->cmd_done(start, ret);
        }
    }
}

static K_WORK_DEFINE(zsr_exec_work, zsr_exec_work_fn);

int zmk_shell_relay_queue_cmd(const char *data, const size_t len) {
    if (len == 0) {
        return -EINVAL;
    }
    if (len >= CONFIG_ZMK_SHELL_RELAY_CMD_BUF_SIZE) {
        return -E2BIG;
    }

    const uint32_t n = (uint32_t)len + 1;
    const k_spinlock_key_t key = k_spin_lock(&zsr_cmd_lock);
    if (ring_buf_space_get(&zsr_cmd_rb) < n) {
        k_spin_unlock(&zsr_cmd_lock, key);
        LOG_WRN("cmd queue full, dropped %zu bytes", len);
        return -ENOSPC;
    }
    const uint8_t nul = 0;
    ring_buf_put(&zsr_cmd_rb, (const uint8_t *)data, (uint32_t)len);
    ring_buf_put(&zsr_cmd_rb, &nul, 1);
    k_spin_unlock(&zsr_cmd_lock, key);

    k_work_submit_to_queue(&zsr_exec_q, &zsr_exec_work);
    return 0;
}

void zmk_shell_relay_reset_cmds(void) {
    const k_spinlock_key_t key = k_spin_lock(&zsr_cmd_lock);
    ring_buf_reset(&zsr_cmd_rb);
    k_spin_unlock(&zsr_cmd_lock, key);
}

static int zsr_init(void) {
    static const struct shell_backend_config_flags cfg_flags = SHELL_DEFAULT_BACKEND_CONFIG_FLAGS;
    shell_init(&zsr_shell, NULL, cfg_flags, false, 0);

    k_work_queue_init(&zsr_exec_q);
    k_work_queue_start(&zsr_exec_q, zsr_exec_stack, K_THREAD_STACK_SIZEOF(zsr_exec_stack),
                       K_PRIO_PREEMPT(CONFIG_ZMK_SHELL_RELAY_EXEC_PRIORITY), NULL);
    k_thread_name_set(&zsr_exec_q.thread, "shell_relay_exec");
    return 0;
}

SYS_INIT(zsr_init, POST_KERNEL, 0);
