package com.vdsirotkin.pgmq

import com.vdsirotkin.pgmq.config.PgmqConnectionFactory
import com.vdsirotkin.pgmq.config.PgmqConfiguration
import com.vdsirotkin.pgmq.domain.PgmqEntry
import com.vdsirotkin.pgmq.domain.PgmqQueue
import com.vdsirotkin.pgmq.serialization.PgmqSerializationProvider
import com.vdsirotkin.pgmq.util.asIterable
import java.sql.ResultSet
import java.time.OffsetDateTime
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

/**
 * PgmqClient is a class that provides an interface to interact with a PostgreSQL message queue.
 *
 * @property connectionFactory The factory to create database connections.
 * @property serializationProvider The provider to serialize and deserialize messages.
 * @property configuration The configuration for the PgmqClient.
 */
class PgmqClient(
    private val connectionFactory: PgmqConnectionFactory,
    private val serializationProvider: PgmqSerializationProvider,
    private val configuration: PgmqConfiguration,
) {

    /**
     * Creates a new queue with the given name.
     *
     * @param queueName The name of the queue to be created.
     */
    fun createQueue(queueName: String) {
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT pgmq.create(?)").use {
                it.setString(1, queueName)
                it.execute()
            }
        }
    }

    /**
     * Drops a queue with the given name.
     *
     * @param queueName The name of the queue to be dropped.
     */
    fun dropQueue(queueName: String) {
        connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT pgmq.drop_queue(?)").use {
                it.setString(1, queueName)
                it.execute()
            }
        }
    }

    /**
     * Lists all the queues.
     *
     * @return A list of all queues.
     */
    fun listQueues(): List<PgmqQueue> {
        return connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT queue_name, created_at FROM pgmq.list_queues()").use {
                it.executeQuery().use { rs ->
                    rs.asIterable {
                        PgmqQueue(
                            queueName = rs.getString("queue_name"),
                            createdAt = rs.getObject("created_at", OffsetDateTime::class.java),
                        )
                    }.toList()
                }
            }
        }
    }

    /**
     * Sends a message to the specified queue.
     *
     * @param queueName The name of the queue.
     * @param message The message to be sent.
     * @param delay The delay before the message becomes visible in the queue.
     * @return The id of the sent message.
     */
    fun send(queueName: String, message: Any, delay: Duration = Duration.ZERO): Long {
        return sendBatch(queueName, listOf(message), delay).firstOrNull() ?: throw PgmqException("No message id provided for sent message. Queue: $queueName, message: $message")
    }

    /**
     * Sends a batch of messages to the specified queue.
     *
     * @param queueName The name of the queue.
     * @param messages The messages to be sent.
     * @param delay The delay before the messages become visible in the queue.
     * @return A list of ids of the sent messages.
     */
    fun <T : Any> sendBatch(queueName: String, messages: Collection<T>, delay: Duration = Duration.ZERO): List<Long> {
        val jsons = messages.map { serializationProvider.serialize(it) }.toTypedArray()
        return connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT send_batch FROM pgmq.send_batch(?, ?::JSONB[], ?)").use {
                it.setString(1, queueName)
                it.setObject(2, jsons)
                it.setInt(3, delay.inWholeSeconds.toInt())
                it.executeQuery().use { rs ->
                    rs.asIterable { rs.getLong("send_batch") }.toList()
                }
            }
        }
    }

    /**
     * Reads a batch of messages from the specified queue.
     *
     * @param queueName The name of the queue.
     * @param quantity The number of messages to read.
     * @param visibilityTimeout The time period during which the message will be invisible to other consumers.
     * @return A list of messages read from the queue.
     */
    fun readBatch(queueName: String, quantity: Int, visibilityTimeout: Duration = configuration.defaultVisibilityTimeout.toKotlinDuration()): List<PgmqEntry> {
        return connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.read(?, ?, ?)").use {
                it.setString(1, queueName)
                it.setInt(2, visibilityTimeout.inWholeSeconds.toInt())
                it.setInt(3, quantity)
                it.executeQuery().use { rs ->
                    rs.asIterable { rs.toPgmqEntry() }.toList()
                }
            }
        }
    }

    /**
     * Pops a message from the specified queue.
     *
     * @param queueName The name of the queue.
     * @return The message popped from the queue.
     */
    fun pop(queueName: String): PgmqEntry? {
        return connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT msg_id, read_ct, enqueued_at, vt, message FROM pgmq.pop(?)").use {
                it.setString(1, queueName)
                it.executeQuery().use { rs ->
                    rs.asIterable { rs.toPgmqEntry() }.firstOrNull()
                }
            }
        }
    }

    /**
     * Archives a list of messages from the specified queue.
     *
     * @param queueName The name of the queue.
     * @param messageIds The ids of the messages to be archived.
     * @return A list of ids of the archived messages.
     */
    fun archive(queueName: String, messageIds: Collection<Long>): List<Long> {
        return connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT archive FROM pgmq.archive(?, ?)").use {
                it.setString(1, queueName)
                it.setObject(2, messageIds.toTypedArray())
                it.executeQuery().use { rs ->
                    rs.asIterable { rs.getLong("archive") }.toList()
                }
            }
        }
    }

    /**
     * Archives a message from the specified queue.
     *
     * @param queueName The name of the queue.
     * @param messageId The id of the message to be archived.
     * @return True if the message was archived, false otherwise.
     */
    fun archive(queueName: String, messageId: Long): Boolean = archive(queueName, listOf(messageId)).size == 1

    /**
     * Deletes a list of messages from the specified queue.
     *
     * @param queueName The name of the queue.
     * @param messageIds The ids of the messages to be deleted.
     * @return A list of ids of the deleted messages.
     */
    fun delete(queueName: String, messageIds: Collection<Long>): List<Long> {
        return connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT delete FROM pgmq.delete(?, ?)").use {
                it.setString(1, queueName)
                it.setObject(2, messageIds.toTypedArray())
                it.executeQuery().use { rs ->
                    rs.asIterable { rs.getLong("delete") }.toList()
                }
            }
        }
    }

    /**
     * Deletes a message from the specified queue.
     *
     * @param queueName The name of the queue.
     * @param messageId The id of the message to be deleted.
     * @return True if the message was deleted, false otherwise.
     */
    fun delete(queueName: String, messageId: Long): Boolean = delete(queueName, listOf(messageId)).size == 1

    /**
     * Purges all messages from the specified queue.
     *
     * @param queueName The name of the queue.
     * @return The number of messages purged from the queue.
     */
    fun purge(queueName: String): Int {
        return connectionFactory.createConnection().use { connection ->
            connection.prepareStatement("SELECT purge_queue from pgmq.purge_queue(?)").use {
                it.setString(1, queueName)
                it.executeQuery().use { rs ->
                    rs.asIterable { it.getInt("purge_queue") }.firstOrNull() ?: 0
                }
            }
        }
    }

    private fun ResultSet.toPgmqEntry() = PgmqEntry(
        messageId = this.getLong("msg_id"),
        readCounter = this.getLong("read_ct"),
        enqueuedAt = this.getObject("enqueued_at", OffsetDateTime::class.java),
        visibilityTime = this.getObject("vt", OffsetDateTime::class.java),
        message = this.getString("message"),
    )
}