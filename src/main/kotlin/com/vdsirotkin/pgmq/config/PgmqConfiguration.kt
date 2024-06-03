package com.vdsirotkin.pgmq.config


interface PgmqConfiguration {
    val defaultVisibilityTimeout: java.time.Duration
}