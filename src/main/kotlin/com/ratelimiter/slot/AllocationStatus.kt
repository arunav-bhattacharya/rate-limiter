package com.ratelimiter.slot

enum class AllocationStatus {
    /** Slot assigned within maxDuration, window below softMax. Normal operation. */
    NORMAL,
    /** Slot assigned within maxDuration, but window is between softMax and hardMax.
     *  All windows in the maxDuration range have reached softMax. */
    SOFT_MAX_EXCEEDED,
    /** Slot assigned beyond the caller's maxDuration.
     *  All windows within maxDuration are completely full (at hardMax). */
    MAX_DURATION_EXCEEDED
}
