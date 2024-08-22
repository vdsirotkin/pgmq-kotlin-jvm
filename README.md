[![codecov](https://codecov.io/gh/vdsirotkin/pgmq-kotlin-jvm/graph/badge.svg?token=GAXNOBXHKX)](https://codecov.io/gh/vdsirotkin/pgmq-kotlin-jvm)
[![Maven Central Version](https://img.shields.io/maven-central/v/com.vdsirotkin.pgmq/pgmq-kotlin-jvm)](https://central.sonatype.com/artifact/com.vdsirotkin.pgmq/pgmq-kotlin-jvm)
![Dynamic XML Badge](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fvdsirotkin%2Fpgmq-kotlin-jvm%2Fmain%2Fpom.xml&query=%2F%2F*%5Blocal-name()%3D'kotlin.version'%5D&label=kotlin)

# pgmq-kotlin-jvm

A Kotlin client, based on pure JDBC, for [Postgres Message Queue](https://github.com/tembo-io/pgmq) (PGMQ).

# Compatibility

This library currently supports following PGMQ API:
* `create`
* `drop_queue`
* `list_queues`
* `send_batch`
* `read`
* `pop`
* `archive`
* `delete`
* `purge_queue`

# Usage

First of all - you need to set up `PgmqConnectionFactory` and `PgmqSerializationProvider`

### PgmqConnectionFactory

You are free to implement it either way you wish :) 

Example implementation for Spring, which acquires current connection inside of `@Transactional` method (may be useful if you need PGMQ to be transational):

```kotlin
@Bean
fun pgmqConnectionFactory(dataSource: DataSource) = PgmqConnectionFactory {
    DataSourceUtils.getConnection(dataSource)
} 
```

Please refer to Spring's [DataSourceUtils](https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/jdbc/datasource/DataSourceUtils.html#getConnection(javax.sql.DataSource)) docs for explanations

### PgmqSerializationProvider

This library provides 2 implementations - [GsonPgmqSerializationProvider](src%2Fmain%2Fkotlin%2Fcom%2Fvdsirotkin%2Fpgmq%2Fserialization%2FGsonPgmqSerializationProvider.kt) and [JacksonPgmqSerializationProvider](src%2Fmain%2Fkotlin%2Fcom%2Fvdsirotkin%2Fpgmq%2Fserialization%2FJacksonPgmqSerializationProvider.kt)

If you use some other serialization library - feel free to implement [PgmqSerializationProvider](src%2Fmain%2Fkotlin%2Fcom%2Fvdsirotkin%2Fpgmq%2Fserialization%2FPgmqSerializationProvider.kt) interface, it's simple! :)

### PgmqConfiguration

Next step should be defining [PgmqConfiguration](src%2Fmain%2Fkotlin%2Fcom%2Fvdsirotkin%2Fpgmq%2Fconfig%2FPgmqConfiguration.kt). You can also implement it either way you want.

Here's an example for Spring's Configuration Properties:

```kotlin
@ConfigurationProperties(prefix = "pgmq")
data class PgmqConfigurationProps(
    override val defaultVisibilityTimeout: java.time.Duration = 30.seconds.toJavaDuration()
) : PgmqConfiguration
```

### Building client

If you have set up connection factory and serializer - you're free to build the client!

Here's complete example of Spring setup for PgmqClient - [SpringPgmqApplication.kt](src%2Ftest%2Fkotlin%2Fcom%2Fvdsirotkin%2Fpgmq%2Fspring%2FSpringPgmqApplication.kt).

# Contributions

Contributions are always welcomed!
