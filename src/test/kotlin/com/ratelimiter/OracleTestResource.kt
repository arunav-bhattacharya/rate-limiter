package com.ratelimiter

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

        // Grant DBMS_RANDOM to the test user — required by the PL/SQL slot assignment block.
        // Oracle XE's default test user may not have this grant.
        try {
            oracle.createConnection("").use { conn ->
                conn.createStatement().execute(
                    "GRANT EXECUTE ON SYS.DBMS_RANDOM TO ${oracle.username}"
                )
            }
        } catch (e: Exception) {
            // Grant may already exist or user may already have access — safe to ignore
        }

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
