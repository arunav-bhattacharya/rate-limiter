package com.ratelimiter

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import org.testcontainers.containers.OracleContainer
import org.testcontainers.utility.DockerImageName
import java.sql.SQLException
import java.time.Duration

/**
 * Testcontainers lifecycle manager that starts an Oracle XE container
 * and injects its connection details into the Quarkus datasource config.
 *
 * Used by all integration tests via `@QuarkusTestResource(OracleTestResource::class)`.
 *
 * Note: On ARM64 hosts (Apple Silicon), this image runs under QEMU emulation
 * and may take 60-120 seconds to start. The startup timeout is set accordingly.
 */
class OracleTestResource : QuarkusTestResourceLifecycleManager {

    companion object {
        private var oracle: OracleContainer = newContainer()

        private fun newContainer(): OracleContainer = OracleContainer(
            DockerImageName.parse("gvenzl/oracle-xe:21-slim-faststart")
        ).apply {
            withReuse(true)
            withStartupTimeout(Duration.ofMinutes(5))
            withConnectTimeoutSeconds(120)
        }

        private fun isHealthy(container: OracleContainer): Boolean = try {
            container.createConnection("").use { conn ->
                conn.createStatement().executeQuery("SELECT 1 FROM DUAL").close()
            }
            true
        } catch (_: SQLException) {
            false
        }
    }

    override fun start(): Map<String, String> {
        oracle.start()

        // Reused containers can become stale (e.g. after system sleep) with
        // the xepdb1 service no longer registered. Detect and replace.
        if (!isHealthy(oracle)) {
            oracle.stop()
            oracle = newContainer()
            oracle.start()
        }

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
