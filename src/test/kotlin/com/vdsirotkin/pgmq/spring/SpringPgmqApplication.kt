package com.vdsirotkin.pgmq.spring

import com.fasterxml.jackson.databind.ObjectMapper
import com.vdsirotkin.pgmq.PgmqClient
import com.vdsirotkin.pgmq.config.PgmqConfiguration
import com.vdsirotkin.pgmq.config.PgmqConnectionFactory
import com.vdsirotkin.pgmq.objectMapper
import com.vdsirotkin.pgmq.serialization.JacksonPgmqSerializationProvider
import com.vdsirotkin.pgmq.serialization.PgmqSerializationProvider
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.datasource.DataSourceUtils
import javax.sql.DataSource
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@SpringBootApplication
@EnableConfigurationProperties(PgmqConfigurationProps::class)
open class SpringPgmqApplication {
    @Configuration(proxyBeanMethods = false)
    open class PgmqConfig {
        @Bean
        open fun objectMapper() = objectMapper

        @Bean
        open fun pgmqConnectionFactory(dataSource: DataSource) = PgmqConnectionFactory {
            DataSourceUtils.getConnection(dataSource)
        }

        @Bean
        open fun pgmqSerializer(objectMapper: ObjectMapper) = JacksonPgmqSerializationProvider(objectMapper)

        @Bean
        open fun pgmqClient(
            pgmqConnectionFactory: PgmqConnectionFactory,
            pgmqSerializationProvider: PgmqSerializationProvider,
            pgmqConfiguration: PgmqConfiguration
        ) = PgmqClient(pgmqConnectionFactory, pgmqSerializationProvider, pgmqConfiguration)
    }
}

@ConfigurationProperties(prefix = "pgmq")
data class PgmqConfigurationProps(
    override val defaultVisibilityTimeout: java.time.Duration = 30.seconds.toJavaDuration()
) : PgmqConfiguration