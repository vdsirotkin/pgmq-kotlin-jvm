package com.vdsirotkin.pgmq.serialization

interface PgmqSerializationProvider {
    fun serialize(obj: Any): String
}