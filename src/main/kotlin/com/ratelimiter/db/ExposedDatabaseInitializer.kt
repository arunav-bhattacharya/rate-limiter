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
import java.sql.Connection
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
            // Exposed defaults to REPEATABLE_READ, which Oracle maps to SERIALIZABLE.
            // SERIALIZABLE breaks the duplicate-key recovery path in claimSlot:
            // the re-read after a constraint violation can't see the row committed
            // by another transaction (snapshot is frozen at txn start).
            // READ_COMMITTED is Oracle's native default and gives per-statement
            // visibility of committed data, which is what FOR UPDATE SKIP LOCKED expects.
            defaultIsolationLevel = Connection.TRANSACTION_READ_COMMITTED
        }
        Database.connect(dataSource, databaseConfig = dbConfig)

        transaction {
            SchemaUtils.create(
                RateLimitConfigTable,
                WindowCounterTable,
                RateLimitEventSlotTable,
                WindowEndTrackerTable
            )

            // Exposed creates sequences for autoIncrement columns but does NOT set
            // DEFAULT on the column itself. Exposed DSL INSERTs reference the sequence
            // explicitly, but raw SQL / PL/SQL INSERTs that omit the column get NULL
            // instead of the next sequence value → ORA-01400.
            // Adding DEFAULT makes the sequence kick in for any INSERT that omits the column.
            exec("ALTER TABLE rate_limit_event_slot MODIFY slot_id DEFAULT rate_limit_event_slot_slot_id_seq.NEXTVAL")
            exec("ALTER TABLE rate_limit_config MODIFY config_id DEFAULT rate_limit_config_config_id_seq.NEXTVAL")
        }
        logger.info("Exposed database schema initialized")
    }
}
