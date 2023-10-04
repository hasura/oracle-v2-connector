package io.hasura.models

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

data class ConfigHeader(
    val value: Map<String, Any> = emptyMap()
) {
    // Allows reading this class from resource method parameters -- @RestHeader, @QueryParam, etc.
    // See: https://quarkus.io/guides/resteasy-reactive#parameter-mapping
    companion object {
        private val objectMapper = jacksonObjectMapper()

        @JvmStatic
        fun fromString(value: String): ConfigHeader = ConfigHeader(objectMapper.readValue(value))
    }
}
