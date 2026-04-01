package com.ratelimiter.temporal

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.schedules.ScheduleClient
import io.temporal.client.schedules.ScheduleClientOptions
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.eclipse.microprofile.config.inject.ConfigProperty

/**
 * CDI producers for Temporal client beans.
 *
 * All three beans are heavyweight, long-lived objects meant to be shared
 * across the application. Created lazily on first injection.
 */
@ApplicationScoped
class TemporalConfig(
    @param:ConfigProperty(name = "temporal.server.target", defaultValue = "localhost:7233")
    private val serverTarget: String,
    @param:ConfigProperty(name = "temporal.namespace", defaultValue = "rate-limiter")
    private val namespace: String
) {

    @Produces
    @ApplicationScoped
    fun workflowServiceStubs(): WorkflowServiceStubs {
        return WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder()
                .setTarget(serverTarget)
                .build()
        )
    }

    @Produces
    @ApplicationScoped
    fun workflowClient(stubs: WorkflowServiceStubs): WorkflowClient {
        return WorkflowClient.newInstance(
            stubs,
            WorkflowClientOptions.newBuilder()
                .setNamespace(namespace)
                .build()
        )
    }

    @Produces
    @ApplicationScoped
    fun scheduleClient(stubs: WorkflowServiceStubs): ScheduleClient {
        return ScheduleClient.newInstance(
            stubs,
            ScheduleClientOptions.newBuilder()
                .setNamespace(namespace)
                .build()
        )
    }
}
