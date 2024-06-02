package com.vdsirotkin.pgmq

import com.vdsirotkin.pgmq.serialization.PgmqSerializationProvider
import java.sql.ResultSet
import java.time.OffsetDateTime
import kotlin.time.Duration

class PgmqClient(
    private val connectionFactory: ConnectionFactory,
    private val serializationProvider: PgmqSerializationProvider,
    private val configuration: PgmqConfiguration,
) {
    fun createQueue(queueName: String) {
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT pgmq.create(?)").use {
                it.setString(1, queueName)
                it.execute()
            }
        }
    }

    fun dropQueue(queueName: String) {
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT pgmq.drop_queue(?)").use {
                it.setString(1, queueName)
                it.execute()
            }
        }
    }

    fun listQueues(): List<PgmqQueue> {
        val result = mutableListOf<PgmqQueue>()
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT queue_name, created_at FROM pgmq.list_queues()").use {
                it.executeQuery().use { rs ->
                    while (rs.next()) {
                        result += PgmqQueue(
                            queueName = rs.getString("queue_name"),
                            createdAt = rs.getObject("created_at", OffsetDateTime::class.java),
                        )
                    }
                }
            }
        }
        return result
    }

    fun send(queueName: String, message: Any, delay: Duration = Duration.ZERO): Long {
        val messageJson = serializationProvider.serialize(message)
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT send FROM pgmq.send(?, ?::JSONB, ?)").use {
                it.setString(1, queueName)
                it.setString(2, messageJson)
                it.setInt(3, delay.inWholeSeconds.toInt())
                it.executeQuery().use { rs ->
                    if (rs.next()) {
                        return rs.getLong("send")
                    }
                }
            }
        }
        throw PgmqException("No message id provided for sent message. Queue: $queueName, message: $messageJson")
    }

    fun <T : Any> sendBatch(queueName: String, messages: Collection<T>, delay: Duration = Duration.ZERO): List<Long> {
        val jsons = messages.map { serializationProvider.serialize(it) }.toTypedArray()
        val result = mutableSetOf<Long>()
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT send_batch FROM pgmq.send_batch(?, ?::JSONB[], ?)").use {
                it.setString(1, queueName)
                it.setObject(2, jsons)
                it.setLong(3, delay.inWholeSeconds)
                it.executeQuery().use { rs ->
                    while (rs.next()) {
                        result += rs.getLong("send_batch")
                    }
                }
            }
        }
        return result.toList()
    }

    fun readBatch(queueName: String, quantity: Int, visibilityTimeout: Duration = configuration.defaultVisibilityTimeout): List<PgmqEntry> {
        val result = mutableListOf<PgmqEntry>()
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read(?, ?, ?)").use {
                it.setString(1, queueName)
                it.setInt(2, visibilityTimeout.inWholeSeconds.toInt())
                it.setInt(3, quantity)
                it.executeQuery().use { rs ->
                    while (rs.next()) {
                        result += rs.toPgmqEntry()
                    }
                }
            }
        }
        return result
    }

    fun pop(queueName: String): PgmqEntry? {
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.pop(?)").use {
                it.setString(1, queueName)
                it.executeQuery().use { rs ->
                    if (rs.next()) {
                        return rs.toPgmqEntry()
                    }
                }
            }
        }
        return null
    }

    fun archive(queueName: String, messageIds: Collection<Long>): List<Long> {
        val result = mutableListOf<Long>()
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT archive FROM pgmq.archive(?, ?)").use {
                it.setString(1, queueName)
                it.setObject(2, messageIds.toTypedArray())
                it.executeQuery().use { rs ->
                    while (rs.next()) {
                        result += rs.getLong("archive")
                    }
                }
            }
        }
        return result
    }

    fun archive(queueName: String, messageId: Long): Boolean = archive(queueName, listOf(messageId)).size == 1

    fun delete(queueName: String, messageIds: Collection<Long>): List<Long> {
        val result = mutableListOf<Long>()
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT delete FROM pgmq.delete(?, ?)").use {
                it.setString(1, queueName)
                it.setObject(2, messageIds.toTypedArray())
                it.executeQuery().use { rs ->
                    while (rs.next()) {
                        result += rs.getLong("delete")
                    }
                }
            }
        }
        return result
    }

    fun delete(queueName: String, messageId: Long): Boolean = delete(queueName, listOf(messageId)).size == 1

    fun purge(queueName: String): Int {
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT purge_queue from pgmq.purge_queue(?)").use {
                it.setString(1, queueName)
                it.executeQuery().use { rs ->
                    if (rs.next()) {
                        return rs.getInt("purge_queue")
                    }
                }
            }
        }
        return 0
    }

    private fun ResultSet.toPgmqEntry() = PgmqEntry(
        messageId = this.getLong("msg_id"),
        readCounter = this.getLong("read_ct"),
        enqueuedAt = this.getObject("enqueued_at", OffsetDateTime::class.java),
        visibilityTime = this.getObject("vt", OffsetDateTime::class.java),
        message = this.getString("message"),
    )
}