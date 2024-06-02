package com.vdsirotkin.pgmq

import kotlin.time.Duration

interface PgmqConfiguration {
    val defaultVisibilityTimeout: Duration
}