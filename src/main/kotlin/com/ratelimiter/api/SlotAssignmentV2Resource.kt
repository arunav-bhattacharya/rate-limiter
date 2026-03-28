package com.ratelimiter.api

import com.ratelimiter.slot.AllocationStatus
import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV5
import jakarta.inject.Inject
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

/**
 * REST endpoint for V5 slot assignment.
 *
 * Exposes the [SlotAssignmentServiceV5] as a synchronous HTTP API.
 * Supports per-request `maxDuration` and returns [AllocationStatus]
 * in the response to indicate which allocation phase produced the slot.
 */
@Path("/api/v2/slots")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class SlotAssignmentV2Resource @Inject constructor(
    private val slotAssignmentService: SlotAssignmentServiceV5
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentV2Resource::class.java)

    /** Request body for V2 slot assignment. */
    data class SlotAssignmentV2Request(
        /** Unique event identifier (idempotency key). */
        val eventId: String,
        /** Desired execution time as ISO-8601 string. */
        val requestedTime: String,
        /** Max duration from requestedTime for slot placement. ISO-8601 duration, e.g., "PT4H". */
        val maxDuration: String? = null
    )

    /** Response body for a successful V2 slot assignment. */
    data class SlotAssignmentV2Response(
        val eventId: String,
        val scheduledTime: String,
        val delayMs: Long,
        val allocationStatus: AllocationStatus
    )

    @POST
    fun assignSlot(request: SlotAssignmentV2Request): Response {
        return try {
            val requestedTime = Instant.parse(request.requestedTime)
            val maxDuration = request.maxDuration?.let { Duration.parse(it) }
                ?: slotAssignmentService.defaultMaxDuration

            val slot = slotAssignmentService.assignSlot(
                eventId = request.eventId,
                requestedTime = requestedTime,
                maxDuration = maxDuration
            )

            Response.ok(slot.toV2Response()).build()
        } catch (e: SlotAssignmentException) {
            logger.error("V5 slot assignment failed for event={}: {}", request.eventId, e.message)
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
            logger.error("Unexpected error in V5 slot assignment for event={}", request.eventId, e)
            Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(mapOf("error" to "Internal server error"))
                .build()
        }
    }

    private fun AssignedSlot.toV2Response() = SlotAssignmentV2Response(
        eventId = eventId,
        scheduledTime = scheduledTime.toString(),
        delayMs = delay.toMillis(),
        allocationStatus = allocationStatus
    )
}
