package com.ratelimiter.slot

import jakarta.enterprise.context.ApplicationScoped
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

/**
 * Proximity-weighted random window selection.
 *
 * weight(W) = capacityWeight × proximityWeight
 *   capacityWeight = max(0, threshold - occupancy(W))
 *   proximityWeight = rangeSize - index  (linear decay)
 *
 * Windows at or above the threshold are excluded (capacityWeight = 0).
 */
@ApplicationScoped
class WindowPicker {

    /**
     * Selects a window from [windows] using combined proximity and capacity weighting.
     * Closer windows with more remaining capacity are proportionally more likely to be selected.
     *
     * Returns null if no window has remaining capacity below [threshold].
     */
    fun pickProximityWeightedRandom(
        windows: List<Instant>,
        occupancy: Map<Instant, Int>,
        threshold: Int
    ): Instant? {
        val rangeSize = windows.size
        val candidates = windows.mapIndexed { index, window ->
            val capacityWeight = maxOf(0, threshold - (occupancy[window] ?: 0))
            val proximityWeight = rangeSize - index
            window to (capacityWeight.toLong() * proximityWeight)
        }.filter { it.second > 0 }

        if (candidates.isEmpty()) return null

        val totalWeight = candidates.sumOf { it.second }
        var roll = ThreadLocalRandom.current().nextLong(totalWeight)
        for ((window, weight) in candidates) {
            roll -= weight
            if (roll < 0) return window
        }
        return candidates.last().first
    }

    /**
     * V7 occupancy-weighted random selection. No proximity factor.
     *
     * weight(W) = max(0, maxSlotsPerWindow - slotCount)
     *
     * Windows with more remaining capacity are proportionally more likely
     * to be selected, naturally spreading load across available windows.
     *
     * @param candidates list of (windowStart, currentSlotCount) from fetchAvailableWindows()
     * @param maxSlotsPerWindow the configured maximum slots per window
     * @return selected window, or null if all candidates are at capacity
     */
    fun pickOccupancyWeightedRandom(
        candidates: List<Pair<Instant, Int>>,
        maxSlotsPerWindow: Int
    ): Instant? {
        val weighted = candidates.map { (window, slotCount) ->
            window to maxOf(0L, (maxSlotsPerWindow - slotCount).toLong())
        }.filter { it.second > 0 }

        if (weighted.isEmpty()) return null

        val totalWeight = weighted.sumOf { it.second }
        var roll = ThreadLocalRandom.current().nextLong(totalWeight)
        for ((window, weight) in weighted) {
            roll -= weight
            if (roll < 0) return window
        }
        return weighted.last().first
    }
}
