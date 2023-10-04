plugins {
    `kotlin-library-conventions`
}

dependencies {
    implementation(project(":gdc-ir"))

    implementation("${GlobalVersions.jooqEdition}:jooq:${GlobalVersions.jooq}")
    implementation("${GlobalVersions.jooqEdition}:jooq-kotlin:${GlobalVersions.jooq}")
    implementation("${GlobalVersions.jooqEdition}:jooq-kotlin-coroutines:${GlobalVersions.jooq}")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:${GlobalVersions.jackson}")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${GlobalVersions.jackson}")
    implementation("com.google.guava:guava:32.1.2-jre") {
        because("Used in QueryRequestRelationGraph to create a graph of table relations")
    }

    implementation(kotlin("script-runtime"))
}

