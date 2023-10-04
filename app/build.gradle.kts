plugins {
    `kotlin-library-conventions`
    kotlin("plugin.allopen") version "1.8.21"

    id("io.quarkus")
    id("com.diffplug.spotless") version "6.10.0"
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

object ProjectVersions {
    const val strikt = "0.34.1"
    const val quarkus_logging_manager = "2.1.4"
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation(project(":gdc-ir"))
    implementation(project(":sql-gen"))
    implementation(kotlin("script-runtime"))

    // NOTE: Changed "enforcedPlatform" to "platform" to allow overriding of versions in the Quarkus BOM
    // for vulnerabilities reported by Aquasec Trivy. This may have unintended consequences.
    implementation(platform("$quarkusPlatformGroupId:$quarkusPlatformArtifactId:$quarkusPlatformVersion"))
    implementation("io.quarkus:quarkus-agroal")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-cache")
    implementation("io.quarkus:quarkus-kotlin")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
    implementation("io.quarkus:quarkus-resteasy-reactive")
    implementation("io.quarkus:quarkus-resteasy-reactive-jackson")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:${GlobalVersions.jackson}")
    implementation("io.quarkus:quarkus-smallrye-fault-tolerance")
    implementation("io.quarkus:quarkus-smallrye-openapi")
    implementation("io.quarkus:quarkus-vertx")
    implementation("io.quarkus:quarkus-reactive-routes")
    implementation("io.quarkus:quarkus-logging-json")

    implementation("io.quarkus:quarkus-opentelemetry")
    implementation("io.opentelemetry:opentelemetry-extension-kotlin")
    implementation("io.opentelemetry:opentelemetry-extension-trace-propagators")
    implementation("io.opentelemetry.instrumentation:opentelemetry-jdbc")

    // JDBC Drivers
    listOf("postgresql", "oracle").forEach {
        implementation("io.quarkus:quarkus-jdbc-$it")
    }
    // Spring JDBC ScriptUtils used for loading .sql files
    implementation("org.springframework:spring-jdbc:6.0.8")

    // JOOQ
    implementation("${GlobalVersions.jooqEdition}:jooq:${GlobalVersions.jooq}")

    // SchemaCrawler
    implementation("us.fatehi:schemacrawler:${GlobalVersions.schemacrawler}")
    listOf("postgresql", "oracle").forEach {
        implementation("us.fatehi:schemacrawler-$it:${GlobalVersions.schemacrawler}")
    }

    // JWT
    implementation("org.bitbucket.b_c:jose4j:${GlobalVersions.jose4j}")

    // //////////////////////
    // Test Dependencies
    // //////////////////////
    testImplementation("io.quarkus:quarkus-junit5")

    // RestAssured
    testImplementation("io.rest-assured:rest-assured")
    testImplementation("io.rest-assured:kotlin-extensions")

    // Testcontainers
    testImplementation(platform("org.testcontainers:testcontainers-bom:${GlobalVersions.testcontainers}"))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:oracle-xe")
}

allOpen {
    annotation("jakarta.ws.rs.Path")
    annotation("jakarta.enterprise.context.ApplicationScoped")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    kotlin {
        ktlint().setUseExperimental(true).editorConfigOverride(
            mapOf("disabled_rules" to "filename")
        )
    }
}

tasks.quarkusDev.configure {
    jvmArgs = Constants.JVM_EXEC_ARGS + listOf(
        // The below is needed to make the Quarkus application reachable
        // from Docker containers when developing inside of WSL2 and run as a standard JVM process
        // (IE so Hasura can reach it)
        "-Djava.net.preferIPv4Stack=true",
        "-Djava.net.preferIPv4Addresses=true",
    )
    compilerOptions {
        compiler("kotlin").args(Constants.KOTLIN_COMPILER_ARGS)
    }
}
