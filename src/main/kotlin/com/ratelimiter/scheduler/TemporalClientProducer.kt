package com.ratelimiter.scheduler

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty

/**
 * CDI producer for the Temporal WorkflowClient.
 * Creates a single shared client instance connected to the configured Temporal server.
 */
@ApplicationScoped
class TemporalClientProducer(
    @param:ConfigProperty(name = "temporal.service-address", defaultValue = "localhost:7233")
    private val serviceAddress: String,
    @param:ConfigProperty(name = "temporal.namespace", defaultValue = "default")
    private val namespace: String
) {

    @Produces
    @Singleton
    fun workflowClient(): WorkflowClient {
        val serviceStubs = WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder()
                .setTarget(serviceAddress)
                .build()
        )

        return WorkflowClient.newInstance(
            serviceStubs,
            WorkflowClientOptions.newBuilder()
                .setNamespace(namespace)
                .build()
        )
    }
}
