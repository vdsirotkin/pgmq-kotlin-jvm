package com.vdsirotkin.pgmq.config

import java.sql.Connection

fun interface ConnectionFactory {
    fun createConnection(): Connection
}