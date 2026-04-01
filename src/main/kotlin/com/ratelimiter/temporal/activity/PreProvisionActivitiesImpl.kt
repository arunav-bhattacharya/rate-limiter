package com.ratelimiter.temporal.activity

import com.ratelimiter.slot.WindowPreProvisioningScheduler

/**
 * Delegates to [WindowPreProvisioningScheduler.provisionWindows] which handles
 * batch insertion, idempotent duplicate-key handling, and MAX(WNDW_STRT_TS)
 * resume logic.
 *
 * Not a CDI bean — constructed manually by [com.ratelimiter.temporal.TemporalWorkerStartup].
 */
class PreProvisionActivitiesImpl(
    private val scheduler: WindowPreProvisioningScheduler
) : PreProvisionActivities {

    override fun provisionWindows() {
        scheduler.provisionWindows()
    }
}
