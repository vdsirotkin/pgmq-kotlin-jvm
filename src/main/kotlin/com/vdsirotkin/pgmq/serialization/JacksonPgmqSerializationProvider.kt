package com.vdsirotkin.pgmq.serialization

import com.fasterxml.jackson.databind.ObjectMapper

class JacksonPgmqSerializationProvider(private val objectMapper: ObjectMapper) : PgmqSerializationProvider {
    override fun serialize(obj: Any): String = objectMapper.writeValueAsString(obj)
}