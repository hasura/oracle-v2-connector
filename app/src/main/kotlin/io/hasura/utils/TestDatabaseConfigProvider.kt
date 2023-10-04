package io.hasura.utils

import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class TestDatabaseConfigProvider {
    @ConfigProperty(name = "testing.database.url.postgres")
    lateinit var postgres: String

    @ConfigProperty(name = "testing.database.url.oracle")
    lateinit var oracle: String
}
