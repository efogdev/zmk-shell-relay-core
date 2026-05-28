/*
 * Copyright (c) 2025 efogdev
 * SPDX-License-Identifier: MIT
 *
 * Shared shell-relay core.
 *
 * Owns a single internal Zephyr shell instance backed by a custom transport
 * whose write() callback feeds a hybrid output buffer (small static ring with
 * heap-backed overflow). Wireless transports (BLE GATT, ESB) attach as the
 * single active sink, execute command lines, and drain output through a
 * no-copy claim/finish API. Only one transport is ever active at a time.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <zephyr/kernel.h>

struct zmk_shell_relay_sink {
    void (*data_ready)(void);
};

void zmk_shell_relay_attach(const struct zmk_shell_relay_sink *sink);
void zmk_shell_relay_detach(void);

void zmk_shell_relay_enqueue(const uint8_t *data, size_t len);

int zmk_shell_relay_execute(const char *cmd);

void zmk_shell_relay_submit_exec(struct k_work *work);

uint32_t zmk_shell_relay_claim(uint8_t **data, uint32_t max_len);
void zmk_shell_relay_finish(uint32_t consumed);
uint32_t zmk_shell_relay_size(void);
void zmk_shell_relay_reset(void);
