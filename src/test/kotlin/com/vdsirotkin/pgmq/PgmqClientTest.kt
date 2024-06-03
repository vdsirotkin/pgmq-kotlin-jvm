package com.vdsirotkin.pgmq

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import com.vdsirotkin.pgmq.domain.PgmqEntry
import com.vdsirotkin.pgmq.serialization.JacksonPgmqSerializationProvider
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.time.Duration.Companion.ZERO
import kotlin.time.Duration.Companion.seconds

@Testcontainers
class PgmqClientTest {

    private val connectionFactory = TestConnectionFactory(pgsql.jdbcUrl, pgsql.username, pgsql.password)
    private val underTest = PgmqClient(connectionFactory, JacksonPgmqSerializationProvider(objectMapper), TestConfiguration)

    @BeforeEach
    fun init() = connectionFactory.prepareConnection()

    @AfterEach
    fun tearDown() = connectionFactory.closeConnection()

    @Test
    fun createQueue() {
        underTest.createQueue("test")

        assertThat(underTest.listQueues()).single().transform { it.queueName }.isEqualTo("test")
    }

    @Test
    fun dropQueue() {
        underTest.createQueue("test")
        assertThat(underTest.listQueues()).hasSize(1)

        underTest.dropQueue("test")
        assertThat(underTest.listQueues()).hasSize(0)
    }

    @Test
    fun listQueues() {
        val queues = listOf("test1", "test2")
        queues.forEach { underTest.createQueue(it) }

        assertThat(underTest.listQueues()).extracting { it.queueName }.isEqualTo(queues)
    }

    @Test
    fun sendAndRead() {
        underTest.createQueue("test")

        underTest.send("test", TestMessage("My cool message"))

        val actual = underTest.readBatch("test", 1)
        assertThat(actual).single().all {
            prop(PgmqEntry::messageId).isEqualTo(1L)
            prop(PgmqEntry::readCounter).isEqualTo(1L)
            prop(PgmqEntry::message).isEqualTo("""{"text": "My cool message"}""")
        }
    }

    @Test
    fun `sendAndRead with visibility timeout`() {
        underTest.createQueue("test")

        underTest.send("test", TestMessage("My cool message"))

        var actual = underTest.readBatch("test", 1, visibilityTimeout = 2.seconds)
        assertThat(actual).single().all {
            prop(PgmqEntry::messageId).isEqualTo(1L)
            prop(PgmqEntry::readCounter).isEqualTo(1L)
            prop(PgmqEntry::message).isEqualTo("""{"text": "My cool message"}""")
        }

        // should return zero now, because message is hidden by visibility timeout
        actual = underTest.readBatch("test", 1, visibilityTimeout = 2.seconds)
        assertThat(actual).hasSize(0)

        Thread.sleep(2000)

        // should be OK now, because timeout has passed
        actual = underTest.readBatch("test", 1, visibilityTimeout = 2.seconds)
        assertThat(actual).hasSize(1)
    }

    @Test
    fun `sendAndRead with delay`() {
        underTest.createQueue("test")

        underTest.send("test", TestMessage("My cool message"), delay = 2.seconds)

        var actual = underTest.readBatch("test", 1)
        assertThat(actual).hasSize(0)

        Thread.sleep(2000)

        actual = underTest.readBatch("test", 1)
        assertThat(actual).single().all {
            prop(PgmqEntry::messageId).isEqualTo(1L)
            prop(PgmqEntry::readCounter).isEqualTo(1L)
            prop(PgmqEntry::message).isEqualTo("""{"text": "My cool message"}""")
        }
    }

    @Test
    fun pop() {
        underTest.createQueue("test_pop")

        underTest.send("test_pop", TestMessage("My cool message"))

        connectionFactory.flushAndRecreate()

        val actual = underTest.pop("test_pop")

        assertThat(actual).isNotNull().all {
            prop(PgmqEntry::messageId).isEqualTo(1L)
            prop(PgmqEntry::readCounter).isEqualTo(0)
            prop(PgmqEntry::message).isEqualTo("""{"text": "My cool message"}""")
        }

        underTest.readBatch("test_pop", 100).also {
            assertThat(it).hasSize(0)
        }

        underTest.dropQueue("test_pop")

        connectionFactory.flush()
    }

    @Test
    fun archive() {
        underTest.createQueue("test")

        val messageId = underTest.send("test", TestMessage("My cool message"))

        val archived = underTest.archive("test", messageId)

        assertThat(archived).isTrue()

        var counter = 0
        connectionFactory.connection.prepareStatement("select * from pgmq.a_test").use {
            it.executeQuery().use { rs ->
                while (rs.next()) {
                    counter++
                }
            }
        }
        assertThat(counter).isEqualTo(1)

        underTest.dropQueue("test")
    }

    @Test
    fun delete() {
        underTest.createQueue("test")

        val messageId = underTest.send("test", TestMessage("My cool message"))

        val archived = underTest.delete("test", messageId)

        assertThat(archived).isTrue()

        var counter = 0
        connectionFactory.connection.prepareStatement("select * from pgmq.a_test").use {
            it.executeQuery().use { rs ->
                while (rs.next()) {
                    counter++
                }
            }
        }
        assertThat(counter).isEqualTo(0)
        connectionFactory.connection.prepareStatement("select * from pgmq.q_test").use {
            it.executeQuery().use { rs ->
                while (rs.next()) {
                    counter++
                }
            }
        }
        assertThat(counter).isEqualTo(0)
    }

    @Test
    fun purge() {
        underTest.createQueue("test")

        underTest.send("test", TestMessage(""))

        var intermediateResult = underTest.readBatch("test", 1, ZERO)
        assertThat(intermediateResult).isNotEmpty()

        val actual = underTest.purge("test")
        assertThat(actual).isEqualTo(1)

        intermediateResult = underTest.readBatch("test", 1, ZERO)
        assertThat(intermediateResult).isEmpty()
    }

    data class TestMessage(val text: String)

    companion object {
        @JvmStatic
        @Container
        val pgsql = PostgreSQLContainer(DockerImageName.parse("quay.io/tembo/pgmq-pg:latest").asCompatibleSubstituteFor(PostgreSQLContainer.IMAGE))
            .withInitScript("init.sql")
    }
}