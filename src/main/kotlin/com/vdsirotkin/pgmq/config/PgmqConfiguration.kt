package com.vdsirotkin.pgmq.config

import kotlin.time.Duration

interface PgmqConfiguration {
    val defaultVisibilityTimeout: Duration
}