package com.vdsirotkin.pgmq

import java.time.OffsetDateTime

data class PgmqQueue(
    val queueName: String,
    val createdAt: OffsetDateTime,
)