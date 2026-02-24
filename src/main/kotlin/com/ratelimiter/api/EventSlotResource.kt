package com.ratelimiter.api

import com.ratelimiter.repo.EventSlotRepository
import jakarta.inject.Inject
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import java.time.Instant

@Path("/api/v1/event-slots")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
class EventSlotResource @Inject constructor(
    private val eventSlotRepository: EventSlotRepository
) {

    data class EventSlotRequest(
        val eventId: String,
        val requestedTime: String,
        val windowStart: String,
        val scheduledTime: String,
        val configId: Long
    )

    @POST
    fun insertEventSlot(request: EventSlotRequest): Response {
        val inserted = eventSlotRepository.insertEventSlotInNewTransaction(
            eventId = request.eventId,
            requestedTime = Instant.parse(request.requestedTime),
            windowStart = Instant.parse(request.windowStart),
            scheduledTime = Instant.parse(request.scheduledTime),
            configId = request.configId
        )

        return if (inserted) {
            Response.status(Response.Status.CREATED)
                .entity(mapOf("eventId" to request.eventId, "status" to "inserted"))
                .build()
        } else {
            Response.status(Response.Status.CONFLICT)
                .entity(mapOf("eventId" to request.eventId, "error" to "Event ID already exists"))
                .build()
        }
    }
}
