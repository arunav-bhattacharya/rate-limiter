package com.payments.ratelimiter

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import org.testcontainers.containers.OracleContainer
import org.testcontainers.utility.DockerImageName

/**
 * Testcontainers lifecycle manager that starts an Oracle XE container
 * and injects its connection details into the Quarkus datasource config.
 *
 * Used by all integration tests via `@QuarkusTestResource(OracleTestResource::class)`.
 */
class OracleTestResource : QuarkusTestResourceLifecycleManager {

    companion object {
        private val oracle: OracleContainer = OracleContainer(
            DockerImageName.parse("gvenzl/oracle-xe:21-slim-faststart")
        ).apply {
            withReuse(true)
        }
    }

    override fun start(): Map<String, String> {
        oracle.start()
        return mapOf(
            "quarkus.datasource.jdbc.url" to oracle.jdbcUrl,
            "quarkus.datasource.username" to oracle.username,
            "quarkus.datasource.password" to oracle.password
        )
    }

    override fun stop() {
        // Container reuse is enabled — don't stop it between test classes
    }
}
