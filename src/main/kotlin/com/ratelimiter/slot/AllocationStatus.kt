package com.ratelimiter.slot

enum class AllocationStatus {
    /** Phase 1: Slot assigned within maxDuration, window below softMax. Normal operation. */
    NORMAL,
    /** Phase 2: Slot assigned within maxDuration, but window is between softMax and maxSlots.
     *  All windows in the maxDuration range have reached softMax. */
    SOFT_MAX_EXCEEDED,
    /** Phase 3: Slot assigned beyond the caller's maxDuration.
     *  All windows within maxDuration are completely full (at maxSlots). */
    MAX_DURATION_EXCEEDED
}
