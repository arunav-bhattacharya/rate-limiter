package com.ratelimiter.api

import com.ratelimiter.slot.AssignedSlot
import com.ratelimiter.slot.SlotAssignmentException
import com.ratelimiter.slot.SlotAssignmentServiceV7
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
import java.time.format.DateTimeParseException

/**
 * REST endpoint for V7 slot assignment.
 *
 * Key difference from V2: [maxDuration] is required (no default).
 */
@Path("/api/v3/slots")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class SlotAssignmentV3Resource @Inject constructor(
    private val slotAssignmentService: SlotAssignmentServiceV7
) {
    private val logger = LoggerFactory.getLogger(SlotAssignmentV3Resource::class.java)

    data class SlotAssignmentV3Request(
        val eventId: String,
        /** ISO-8601 instant, e.g., "2025-06-01T12:00:00Z". */
        val requestedTime: String,
        /** ISO-8601 duration, e.g., "PT8H". Required. */
        val maxDuration: String
    )

    data class SlotAssignmentV3Response(
        val eventId: String,
        val scheduledTime: String,
        val delayMs: Long
    )

    @POST
    fun assignSlot(request: SlotAssignmentV3Request): Response {
        return try {
            val requestedTime = Instant.parse(request.requestedTime)
            val maxDuration = Duration.parse(request.maxDuration)

            if (maxDuration.isNegative || maxDuration.isZero) {
                return Response.status(Response.Status.BAD_REQUEST)
                    .entity(mapOf("error" to "maxDuration must be positive"))
                    .build()
            }

            val slot = slotAssignmentService.assignSlot(
                eventId = request.eventId,
                requestedTime = requestedTime,
                maxDuration = maxDuration
            )

            Response.ok(slot.toV3Response()).build()
        } catch (e: DateTimeParseException) {
            Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Invalid date/duration format: ${e.message}"))
                .build()
        } catch (e: SlotAssignmentException) {
            logger.error("V7 slot assignment failed for event={}: {}", request.eventId, e.message)
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
            logger.error("Unexpected error in V7 slot assignment for event={}", request.eventId, e)
            Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(mapOf("error" to "Internal server error"))
                .build()
        }
    }

    private fun AssignedSlot.toV3Response() = SlotAssignmentV3Response(
        eventId = eventId,
        scheduledTime = scheduledTime.toString(),
        delayMs = delay.toMillis()
    )
}
