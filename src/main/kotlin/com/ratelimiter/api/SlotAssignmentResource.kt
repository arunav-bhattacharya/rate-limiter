package com.ratelimiter.api

import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV3
import jakarta.inject.Inject
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.slf4j.LoggerFactory
import java.time.Instant

/**
 * REST endpoint for slot assignment.
 *
 * Exposes the [SlotAssignmentServiceV3] as a synchronous HTTP API.
 * Callers POST a slot assignment request and receive the assigned
 * slot with event ID, scheduled time, and delay from requested time.
 */
@Path("/api/v1/slots")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class SlotAssignmentResource @Inject constructor(
    private val slotAssignmentService: SlotAssignmentServiceV3
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentResource::class.java)

    /** Request body for slot assignment. */
    data class SlotAssignmentRequest(
        /** Unique event identifier (idempotency key). */
        val eventId: String,
        /** Desired execution time as ISO-8601 string. */
        val requestedTime: String
    )

    /** Response body for a successful slot assignment. */
    data class SlotAssignmentResponse(
        val eventId: String,
        val scheduledTime: String,
        val delayMs: Long
    )

    /**
     * Assign a rate-limited slot for the given event.
     *
     * Usage: POST /api/v1/slots
     * ```json
     * {
     *   "eventId": "pay-123",
     *   "requestedTime": "2025-06-01T12:00:00Z"
     * }
     * ```
     *
     * Returns the assigned slot with scheduled time and delay.
     * Idempotent: calling with the same eventId returns the same slot.
     */
    @POST
    fun assignSlot(request: SlotAssignmentRequest): Response {
        return try {
            val requestedTime = Instant.parse(request.requestedTime)
            val slot = slotAssignmentService.assignSlot(
                eventId = request.eventId,
                requestedTime = requestedTime
            )

            Response.ok(slot.toResponse()).build()
        } catch (e: SlotAssignmentException) {
            logger.error("Slot assignment failed for event={}: {}", request.eventId, e.message)
            Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity(
                    mapOf(
                        "error" to e.message,
                        "eventId" to e.eventId,
                        "windowsSearched" to e.windowsSearched
                    )
                )
                .build()
        } catch (e: Exception) {
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
}
