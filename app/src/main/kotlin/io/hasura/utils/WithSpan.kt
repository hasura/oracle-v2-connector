package io.hasura.utils

import io.opentelemetry.api.trace.Tracer

fun <T> withSpan(tracer: Tracer, name: String, block: () -> T): T {
    val span = tracer.spanBuilder(name).startSpan()
    return try {
        span.makeCurrent().use { block() }
    } finally {
        span.end()
    }
}
