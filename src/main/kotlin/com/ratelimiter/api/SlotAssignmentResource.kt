package com.ratelimiter.api

import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.AssignmentOverloadedException
import com.ratelimiter.slot.ConfigLoadException
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentService
import com.ratelimiter.slot.SlotAssignmentSql
import jakarta.inject.Inject
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.slf4j.LoggerFactory
import java.sql.SQLTransientConnectionException
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * REST endpoint for slot assignment.
 */
@Path("/api/v1/slots")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class SlotAssignmentResource @Inject constructor(
    private val slotAssignmentService: SlotAssignmentService,
    @param:ConfigProperty(name = "rate-limiter.max-request-ahead-minutes", defaultValue = "60")
    private val maxRequestAheadMinutes: Long
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentResource::class.java)

    /** Request body for slot assignment. */
    data class SlotAssignmentRequest(
        val eventId: String,
        val configName: String,
        val requestedTime: String,
        val resumeFromWindow: String? = null
    )

    /** Response body for a successful slot assignment. */
    data class SlotAssignmentResponse(
        val eventId: String,
        val scheduledTime: String,
        val delayMs: Long
    )

    @POST
    fun assignSlot(request: SlotAssignmentRequest): Response {
        return try {
            val requestedTime = parseInstant(request.requestedTime, "requestedTime")
            val maxAllowedRequestedTime = Instant.now().plusSeconds(maxRequestAheadMinutes * 60)
            if (requestedTime.isAfter(maxAllowedRequestedTime)) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(
                        mapOf(
                            "error" to "requestedTime must be within $maxRequestAheadMinutes minutes of now",
                            "maxAllowedRequestedTime" to maxAllowedRequestedTime.toString()
                        )
                    )
                    .build()
            }

            val resumeFromWindow = request.resumeFromWindow?.let { parseInstant(it, "resumeFromWindow") }
            val slot = slotAssignmentService.assignSlot(
                eventId = request.eventId,
                configName = request.configName,
                requestedTime = requestedTime,
                resumeFromWindow = resumeFromWindow
            )

            Response.ok(slot.toResponse()).build()
        } catch (e: ConfigLoadException) {
            logger.warn("Config not found: {}", e.configName)
            Response.status(Response.Status.NOT_FOUND)
                .entity(mapOf("error" to e.message))
                .build()
        } catch (e: AssignmentOverloadedException) {
            logger.warn("Slot assignment overloaded for event={}: {}", request.eventId, e.message)
            val retryAfterMs = if (e.retryAfterMs > 0) e.retryAfterMs else 1000L
            val payload = mapOf(
                "error" to e.message,
                "eventId" to request.eventId,
                "reason" to SlotAssignmentSql.REASON_OVERLOADED,
                "retryAfterMs" to retryAfterMs
            )
            withRetryAfter(
                Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(payload),
                retryAfterMs
            ).build()
        } catch (e: SlotAssignmentException) {
            logger.error("Slot assignment failed for event={}: {}", request.eventId, e.message)
            val retryAfterMs = calculateRetryAfterMs(e.resumeFromWindow, fallbackMs = 1000L)
            val payload = mapOf(
                "error" to e.message,
                "eventId" to e.eventId,
                "windowsSearched" to e.windowsSearched,
                "reason" to e.reason,
                "resumeFromWindow" to e.resumeFromWindow?.toString(),
                "searchLimit" to e.searchLimit?.toString(),
                "retryAfterMs" to retryAfterMs
            )
            withRetryAfter(
                Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(payload),
                retryAfterMs
            ).build()
        } catch (e: IllegalArgumentException) {
            Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to (e.message ?: "Invalid request")))
                .build()
        } catch (e: Exception) {
            if (isTransientConnectionIssue(e)) {
                val retryAfterMs = 1000L
                val payload = mapOf(
                    "error" to "Service overloaded",
                    "eventId" to request.eventId,
                    "reason" to SlotAssignmentSql.REASON_OVERLOADED,
                    "retryAfterMs" to retryAfterMs
                )
                return withRetryAfter(
                    Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(payload),
                    retryAfterMs
                ).build()
            }

            logger.error("Unexpected error assigning slot for event={}", request.eventId, e)
            Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(mapOf("error" to "Internal server error"))
                .build()
        }
    }

    private fun AssignedSlot.toResponse() = SlotAssignmentResponse(
        eventId = eventId,
        scheduledTime = scheduledTime.toString(),
        delayMs = delay.toMillis()
    )

    private fun calculateRetryAfterMs(
        resumeFromWindow: Instant?,
        fallbackMs: Long
    ): Long {
        if (resumeFromWindow == null) return fallbackMs
        val deltaMs = Duration.between(Instant.now(), resumeFromWindow).toMillis()
        return if (deltaMs > 0) deltaMs else fallbackMs
    }

    private fun withRetryAfter(
        builder: Response.ResponseBuilder,
        retryAfterMs: Long
    ): Response.ResponseBuilder {
        val seconds = ((retryAfterMs + 999) / 1000).coerceAtLeast(1)
        return builder.header("Retry-After", seconds)
    }

    private fun parseInstant(value: String, field: String): Instant = try {
        Instant.parse(value)
    } catch (_: DateTimeParseException) {
        throw IllegalArgumentException("$field must be an ISO-8601 instant")
    }

    private fun isTransientConnectionIssue(error: Throwable): Boolean {
        var current: Throwable? = error
        while (current != null) {
            if (current is SQLTransientConnectionException) {
                return true
            }
            val message = current.message?.lowercase().orEmpty()
            if ("acquisition timeout" in message || "unable to acquire" in message) {
                return true
            }
            current = current.cause
        }
        return false
    }
}
