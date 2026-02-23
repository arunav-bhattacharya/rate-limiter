package com.ratelimiter.slot

import jakarta.enterprise.context.ApplicationScoped
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Duration
import java.time.Instant

@ApplicationScoped
class SlotAssignmentServiceV2 {

    fun assignSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        return fetchAssignedSlot(eventId) ?: assignNewSlot(eventId, requestedTime)
    }

    private fun assignNewSlot(eventId: String, requestedTime: Instant): AssignedSlot {
        // Get the current window to search
        val currWindowEnd = currentWindowEnd(requestedTime)

        return transaction {
            val firstAvailableSlot = lockFirstAvailableSlot(requestedTime, currWindowEnd)

            if (firstAvailableSlot != null) {
                claimSlot(eventId, requestedTime, requestedTime)
            } else {
                // No available slots found, add another chunk of windows
                val nextWindowStart = currWindowEnd + WINDOW_SIZE
                val newWindowEnd = currWindowEnd + WINDOW_CHUNK_SIZE
                loadChunkOfWindows(nextWindowStart)
                updateWindowEnd(requestedTime, currWindowEnd, newWindowEnd)

                // Try to claim a slot again after loading a new chunk of windows
                lockFirstAvailableSlot(requestedTime, newWindowEnd)
                    ?: throw RuntimeException(
                        "Couldn't find a slot to assign for eventId: " +
                                "$eventId, requestedTime: $requestedTime. No existing windows, and failed to assign in new window."
                    )
                claimSlot(eventId, requestedTime, requestedTime)
            }
        }
    }

    private fun currentWindowEnd(requestedTime: Instant): Instant {
        return fetchWindowEnd(requestedTime) ?: run {
            // No existing windows, insert next chunk of windows
            loadChunkOfWindows(requestedTime)
            // Keep track of the window end for this requested time,
            // so that we can time-window the look-up
            val windowEnd = requestedTime + WINDOW_CHUNK_SIZE
            insertWindowEnd(requestedTime, windowEnd)
            windowEnd
        }
    }

    private fun loadChunkOfWindows(windowStart: Instant) {
        val windowsToInsert = mutableListOf<Instant>()
        for (i in 0 until MAX_WINDOWS_IN_CHUNK) {
            windowsToInsert.add(windowStart)
            windowStart.plus(WINDOW_SIZE)
        }
        batchInsertWindows(windowsToInsert)
    }

    private fun batchInsertWindows(windowsToInsert: MutableList<Instant>) {
        /*
        *   INSERT ALL
        *      INTO WINDOW_COUNTER (WINDOW_START, SLOT_COUNT) VALUES (windowStart, 0)
        *
        * */
        TODO("Not yet implemented")
    }

    private fun claimSlot(
        eventId: String,
        requestedTime: Instant,
        window: Instant
    ): AssignedSlot {
        /*
        *   INSERT INTO EVENT_SLOT VALUES (eventId, window, scheduledTime, jitter)
        *
        *   IF INSERT FAILED DUE TO DUPLICATE KEY THEN
        *      SELECT SLOT_ID, SCHEDULED_TIME, JITTER FROM EVENT_SLOT WHERE EVENT_ID = eventId
        *
        *   UPDATE WINDOW_COUNTER SET SLOT_COUNT = SLOT_COUNT + 1 WHERE WINDOW_START = window
        *
        * */

        TODO("Not yet implemented")
    }

    private fun lockFirstAvailableSlot(windowStart: Instant, windowEnd: Instant): Instant? {
        /*
        *   SELECT FOR UPDATE WINDOW_START
        *   FROM   WINDOW_COUNTER
        *   WHERE
        *          WINDOW_START >= windowStart
        *   AND    WINDOW_START <= windowEnd
        *   AND    SLOT_COUNT < MAX_SLOTS_PER_WINDOW
        *   ORDER BY WINDOW_START ASC
        *   FETCH FIRST 1 ROW ONLY
        *   SKIP LOCKED;
        *
        *
        *   fetchFirstWindowHavingAvailableSlot()
        * */

        // Return the window_start from the above query, or null if no windows are available

        // TODO: This is temporary. Remove it once proper implementation is done.
        return Instant.now()
    }


    private fun insertWindowEnd(
        requestedTime: Instant,
        windowEnd: Instant,
    ) {
        // INSERT INTO WINDOW_END VALUES (requestedTime, windowStart)
        // IF DUP INDEX - IGNORE
    }

    private fun fetchWindowEnd(requestedTime: Instant): Instant? {
        /*
        *
        *   SELECT WINDOW_END
        *   FROM WINDOW_END
        *   WHERE REQUESTED_TIME = requestedTime
        *
        * */
        // TODO: This is temporary. Remove it once proper implementation is done.
        return requestedTime
    }

    private fun updateWindowEnd(
        requestedTime: Instant,
        currentWindowEnd: Instant,
        newWindowEnd: Instant
    ) {
        /*
        *   UPDATE WINDOW_END
        *   SET    WINDOW_END = newWindowEnd
        *   WHERE REQUESTED_TIME = requestedTime
        *   AND   WINDOW_END = currentWindowEnd
        *
        * */
    }

    private fun fetchAssignedSlot(eventId: String): AssignedSlot? {
        /*
        *
        *   SELECT SLOT_ID, SCHEDULED_TIME, JITTER
        *   FROM EVENT_SLOT
        *
        * */

        // TODO: This is temporary. Remove it once proper implementation is done.
        return AssignedSlot(
            eventId = "event123",
            scheduledTime = Instant.now(),
            delay = Duration.ofSeconds(10)
        )
    }

    companion object {
        private val WINDOW_SIZE = Duration.ofSeconds(4)
        private val HEADROOM = Duration.ofMinutes(15)
        private val CONFIG_MAX_WINDOWS_IN_CHUNK = 1000L
        private val MAX_WINDOWS_IN_CHUNK =
            minOf(CONFIG_MAX_WINDOWS_IN_CHUNK, 100L) // enforce an upper bound to limit Oracle batch inserts
        private val WINDOW_CHUNK_SIZE = WINDOW_SIZE.multipliedBy(MAX_WINDOWS_IN_CHUNK)

    }
}