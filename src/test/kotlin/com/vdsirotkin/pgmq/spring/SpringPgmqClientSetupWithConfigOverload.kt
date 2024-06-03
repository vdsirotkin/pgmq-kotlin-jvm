package com.vdsirotkin.pgmq.spring

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.vdsirotkin.pgmq.PgmqClient
import com.vdsirotkin.pgmq.config.PgmqConfiguration
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@SpringBootTest
@ActiveProfiles("test") // activates application-test.properties
class SpringPgmqClientSetupWithConfigOverload(
    @Autowired private val pgmqClient: PgmqClient,
    @Autowired private val pgmqConfiguration: PgmqConfiguration
) {
    @Test
    fun `context starts up`(){
        assertThat(pgmqConfiguration.defaultVisibilityTimeout).isEqualTo(10.seconds.toJavaDuration())
    }
}