package com.ratelimiter.db

import io.quarkus.runtime.Startup
import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.DatabaseConfig
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import javax.sql.DataSource

/**
 * Connects the Kotlin Exposed library to the Quarkus-managed Agroal DataSource
 * and creates the schema if it doesn't already exist.
 *
 * Replaces Flyway: tables, indexes, and constraints are defined in the Exposed
 * table objects and created via [SchemaUtils.create].
 *
 * The @Startup annotation ensures this bean is eagerly initialized at application start,
 * since nothing injects it directly.
 */
@Startup
@ApplicationScoped
class ExposedDatabaseInitializer @Inject constructor(
    private val dataSource: DataSource
) {
    private val logger = LoggerFactory.getLogger(ExposedDatabaseInitializer::class.java)

    @PostConstruct
    fun init() {
        val dbConfig = DatabaseConfig {
            defaultMaxAttempts = 1          // No retries (1 attempt only)
            defaultMinRetryDelay = 0
            defaultMaxRetryDelay = 0
        }
        Database.connect(dataSource, databaseConfig = dbConfig)

        transaction {
            SchemaUtils.create(
                RateLimitConfigTable,
                WindowCounterTable,
                RateLimitEventSlotTable,
                WindowEndTrackerTable
            )
        }
        logger.info("Exposed database schema initialized")
    }
}
