package com.ratelimiter.temporal.activity

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

/**
 * Temporal activity for window pre-provisioning.
 * Shared by V7 and V8 — both depend on pre-provisioned window counter rows.
 */
@ActivityInterface
interface PreProvisionActivities {

    /**
     * Provisions window counter rows up to [preProvisionDays] ahead.
     * Idempotent: duplicate keys caught silently.
     * Delegates to [com.ratelimiter.slot.WindowPreProvisioningScheduler.provisionWindows].
     */
    @ActivityMethod
    fun provisionWindows()
}
