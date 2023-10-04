plugins {
    `java-library`
    kotlin("jvm")
}

group = "com.example"
version = "1.0.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of("17"))
    }
}

repositories {
    mavenCentral()
    maven("${project.rootProject.projectDir.absolutePath}/lib/m2repo")
    mavenLocal()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))

    testImplementation(platform("org.junit:junit-bom:${GlobalVersions.junit}"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // Strikt
    testImplementation(platform("io.strikt:strikt-bom:${GlobalVersions.strikt}"))
    testImplementation("io.strikt:strikt-jackson")
    testImplementation("io.strikt:strikt-jvm")
}

tasks.withType<Test> {
    useJUnitPlatform {
        excludeTags("integration")
        excludeTags("slow")
    }
}

tasks.register("integrationTest", Test::class) {
    useJUnitPlatform {
        includeTags("integration")
    }
}

tasks.register("slowTest", Test::class) {
    useJUnitPlatform {
        includeTags("slow")
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions.javaParameters = true
    kotlinOptions.freeCompilerArgs = Constants.KOTLIN_COMPILER_ARGS
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("--enable-preview")
    options.compilerArgs.add("-Xlint:preview")
}

val JVM_EXEC_CONFIG = Action<JavaForkOptions> { jvmArgs(Constants.JVM_EXEC_ARGS) }

tasks.withType<JavaExec>().configureEach(JVM_EXEC_CONFIG)
tasks.withType<Test>().configureEach(JVM_EXEC_CONFIG)
