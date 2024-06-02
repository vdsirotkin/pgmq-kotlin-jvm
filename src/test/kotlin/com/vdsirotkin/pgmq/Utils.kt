package com.vdsirotkin.pgmq

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.vdsirotkin.pgmq.PgmqClientTest.Companion.pgsql
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

object TestConfiguration : PgmqConfiguration {
    override val defaultVisibilityTimeout: Duration = 10.seconds
}

val objectMapper = jacksonObjectMapper().findAndRegisterModules()


class TestConnection(private val connection: Connection) : Connection by connection {
    override fun close() {
        logger.info("Ignoring connection close")
    }

    fun realClose() {
        logger.info("Closing connection")
        connection.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TestConnection::class.java)
    }
}

class TestConnectionFactory(private val jdbcUrl: String, private val username: String, private val password: String,) : ConnectionFactory {
    lateinit var connection: TestConnection

    fun prepareConnection() {
        logger.info("Preparing connection")
        connection = DriverManager.getConnection(jdbcUrl, username, password).apply {
            autoCommit = false
        }.let { TestConnection(it) }
    }

    fun closeConnection() {
        connection.realClose()
    }

    fun flush() {
        connection.commit()
        closeConnection()
    }

    fun flushAndRecreate() {
        flush()
        prepareConnection()
    }

    override fun createConnection(): Connection {
        return connection
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TestConnectionFactory::class.java)
    }
}