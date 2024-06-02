package com.vdsirotkin.pgmq

import java.sql.Connection

fun interface ConnectionFactory {
    fun createConnection(): Connection
}