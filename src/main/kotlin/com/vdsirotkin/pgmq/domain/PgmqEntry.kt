package com.vdsirotkin.pgmq.domain

import java.time.OffsetDateTime

data class PgmqEntry(
    val messageId: Long,
    val readCounter: Long,
    val enqueuedAt: OffsetDateTime,
    val visibilityTime: OffsetDateTime,
    val message: String,
)