package com.vdsirotkin.pgmq.domain

import java.time.OffsetDateTime

data class PgmqQueue(
    val queueName: String,
    val createdAt: OffsetDateTime,
)