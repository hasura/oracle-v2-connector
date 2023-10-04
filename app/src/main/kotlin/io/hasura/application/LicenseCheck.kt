package io.hasura.application

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import jakarta.inject.Inject
import jakarta.ws.rs.container.ContainerRequestContext
import jakarta.ws.rs.container.ContainerRequestFilter
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.Provider
import org.jboss.logging.Logger
import org.jose4j.jwk.JsonWebKey
import org.jose4j.jwt.consumer.InvalidJwtException
import org.jose4j.jwt.consumer.JwtConsumerBuilder
import org.jose4j.keys.resolvers.JwksVerificationKeyResolver

@Provider
class LicenseCheck : ContainerRequestFilter {
    private val jwksResolver: JwksVerificationKeyResolver

    private val paths: List<String> = listOf("explain", "query", "mutation")

    @Inject
    private lateinit var logger: Logger

    init {
        // set JwksVerificationKeyResolver from keys in json file
        val mapper = jacksonObjectMapper()
        mapper.registerKotlinModule()
        val jwkJson = LicenseCheck::class.java.getResource("/ee-lite-jwk.json")!!.readText()
        val keyList = mapper.readValue<Map<String, List<Map<String, Object>>>>(jwkJson)["keys"]!!
        val jwks = keyList.map { JsonWebKey.Factory.newJwk(it) }
        jwksResolver = JwksVerificationKeyResolver(jwks)
    }

    override fun filter(requestContext: ContainerRequestContext) {
        val path = requestContext.uriInfo.pathSegments.last().path.lowercase()
        if (paths.contains(path)) {
            val license =
                requestContext.headers.getFirst("X-Hasura-License") ?: System.getenv("HASURA_LICENSE_KEY") ?: ""
            if (!isValid(license)) {
                requestContext.abortWith(
                    Response
                        .status(Response.Status.UNAUTHORIZED)
                        .build()
                )
            }
        }
    }

    private fun isValid(license: String): Boolean {
        if (license.isEmpty()) return false

        val jwtConsumer = JwtConsumerBuilder()
            .setRequireExpirationTime()
            .setAllowedClockSkewInSeconds(30)
            .setVerificationKeyResolver(jwksResolver)
            .setRequireSubject()
            .setExpectedIssuer("Hasura")
            .build()

        return try {
            jwtConsumer.process(license)
            true
        } catch (e: InvalidJwtException) {
            logger.error("Validation Error for license:\n\n$license\n\n", e)
            false
        }
    }
}
