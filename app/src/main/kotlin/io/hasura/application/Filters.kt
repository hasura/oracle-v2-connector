package io.hasura.application

import io.vertx.core.http.HttpServerRequest
import jakarta.inject.Inject
import jakarta.ws.rs.container.ContainerRequestContext
import jakarta.ws.rs.core.UriInfo
import org.jboss.logging.Logger
import org.jboss.resteasy.reactive.server.ServerRequestFilter

class Filters {

    @Inject
    private lateinit var logger: Logger

    @ServerRequestFilter(priority = 0)
    fun logBodyFilter(info: UriInfo, request: HttpServerRequest, ctx: ContainerRequestContext) {
        request.body { b ->
            logger.debug("INCOMING IR: ${b.result()}")
        }
    }

    // Requested by Haskell team, to be able to test/debug error behavior
    @ServerRequestFilter(priority = 1)
    fun crashNowFilter(info: UriInfo, request: HttpServerRequest, ctx: ContainerRequestContext) {
        if (request.headers().contains("X-Hasura-DataConnector-CrashNow")) {
            val msg = request.getHeader("X-Hasura-DataConnector-CrashNow-Message")
            throw RuntimeException(msg ?: "CrashNow Default Message")
        }
    }
}
