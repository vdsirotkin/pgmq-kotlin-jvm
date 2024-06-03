package com.vdsirotkin.pgmq.serialization

import com.google.gson.Gson

class GsonPgmqSerializationProvider(private val gson: Gson) : PgmqSerializationProvider {
    override fun serialize(obj: Any): String = gson.toJson(obj)
}