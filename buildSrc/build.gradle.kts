plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

val kotlinVersion = "1.8.21"

dependencies {
    // These two are needed or else the wrong versions of them may be used by kotlin-dsl plugin
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:$kotlinVersion")
    implementation("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")

    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:$kotlinVersion")
}
