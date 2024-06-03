package com.vdsirotkin.pgmq.config

import java.sql.Connection

fun interface PgmqConnectionFactory {
    fun createConnection(): Connection
}