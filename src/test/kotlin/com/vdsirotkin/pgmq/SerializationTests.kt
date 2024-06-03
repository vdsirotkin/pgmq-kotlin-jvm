package com.vdsirotkin.pgmq

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.google.gson.Gson
import com.vdsirotkin.pgmq.serialization.GsonPgmqSerializationProvider
import com.vdsirotkin.pgmq.serialization.JacksonPgmqSerializationProvider
import org.junit.jupiter.api.Test

class SerializationTests {

    @Test
    fun jackson() {
        val provider = JacksonPgmqSerializationProvider(objectMapper)
        val result = provider.serialize(TestMessage("some text"))

        assertThat(result).isEqualTo("""{"text":"some text"}""")
    }

    @Test
    fun gson() {
        val provider = GsonPgmqSerializationProvider(Gson())
        val result = provider.serialize(TestMessage("some text"))

        assertThat(result).isEqualTo("""{"text":"some text"}""")
    }

    data class TestMessage(val text: String)
}