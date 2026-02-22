package com.ratelimiter.admin

import com.ratelimiter.config.RateLimitConfig
import com.ratelimiter.config.RateLimitConfigRepository
import jakarta.inject.Inject
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * Admin REST endpoint for managing rate limit configuration.
 * Provides config CRUD and cache management operations.
 */
@Path("/admin/rate-limit")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class RateLimitAdminResource @Inject constructor(
    private val configRepository: RateLimitConfigRepository
) {

    /** Request body for creating/updating config. */
    data class ConfigRequest(
        val configName: String,
        val maxPerWindow: Int,
        /** ISO-8601 duration string, e.g., "PT4S" for 4 seconds. */
        val windowSize: String,
        /** Optional per-config override for search headroom (in windows). */
        val headroomWindows: Int? = null
    )

    /** Response body for config queries. */
    data class ConfigResponse(
        val configId: Long,
        val configName: String,
        val maxPerWindow: Int,
        /** ISO-8601 duration string. */
        val windowSize: String,
        val headroomWindows: Int?,
        val effectiveFrom: String,
        val isActive: Boolean,
        val createdAt: String
    )

    /**
     * Get the active config for a given name.
     * Usage: GET /admin/rate-limit/config?name=default
     */
    @GET
    @Path("/config")
    fun getConfig(@QueryParam("name") name: String?): Response {
        val configName = name ?: "default"
        val config = configRepository.loadActiveConfig(configName)
            ?: return Response.status(Response.Status.NOT_FOUND)
                .entity(mapOf("error" to "No active config found for: $configName"))
                .build()

        return Response.ok(config.toResponse()).build()
    }

    /**
     * Create a new config version (deactivates previous active config for the same name).
     * Usage: POST /admin/rate-limit/config
     */
    @POST
    @Path("/config")
    fun createConfig(request: ConfigRequest): Response {
        if (request.maxPerWindow <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "maxPerWindow must be positive"))
                .build()
        }

        val windowSize = try {
            Duration.parse(request.windowSize)
        } catch (e: DateTimeParseException) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Invalid windowSize format. Use ISO-8601 duration (e.g., 'PT4S')"))
                .build()
        }

        if (windowSize.isZero || windowSize.isNegative) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "windowSize must be positive"))
                .build()
        }

        val headroomWindows = request.headroomWindows
        if (headroomWindows != null && headroomWindows <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "headroomWindows must be positive when provided"))
                .build()
        }

        val config = try {
            configRepository.createConfig(
                configName = request.configName,
                maxPerWindow = request.maxPerWindow,
                windowSize = windowSize,
                headroomWindows = headroomWindows,
                effectiveFrom = Instant.now()
            )
        } catch (e: IllegalArgumentException) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to (e.message ?: "Invalid configuration")))
                .build()
        }

        return Response.status(Response.Status.CREATED)
            .entity(config.toResponse())
            .build()
    }

    /**
     * Flush the in-memory config cache on all nodes.
     * Usage: POST /admin/rate-limit/cache/flush
     */
    @POST
    @Path("/cache/flush")
    fun flushCache(): Response {
        configRepository.evictCache()
        return Response.ok(mapOf("status" to "cache flushed")).build()
    }

    private fun RateLimitConfig.toResponse() = ConfigResponse(
        configId = configId,
        configName = configName,
        maxPerWindow = maxPerWindow,
        windowSize = windowSize.toString(),
        headroomWindows = headroomWindows,
        effectiveFrom = effectiveFrom.toString(),
        isActive = isActive,
        createdAt = createdAt.toString()
    )
}
