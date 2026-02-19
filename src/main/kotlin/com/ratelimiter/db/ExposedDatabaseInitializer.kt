package com.ratelimiter.db

import jakarta.annotation.PostConstruct
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jetbrains.exposed.sql.Database
import javax.sql.DataSource

/**
 * Connects the Kotlin Exposed library to the Quarkus-managed Agroal DataSource.
 * Without this, all `transaction {}` blocks will fail with "Database not found".
 */
@ApplicationScoped
class ExposedDatabaseInitializer @Inject constructor(
    private val dataSource: DataSource
) {
    @PostConstruct
    fun init() {
        Database.connect(dataSource)
    }
}
