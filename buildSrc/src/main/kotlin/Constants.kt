object Constants {
    val JVM_EXEC_ARGS = listOf(
        "--enable-preview",
        // The below is required for Apache Arrow to work properly in recent JDK
        // Arrow is used in Snowflake's JDBC Driver
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "--illegal-access=permit"
    )
    val KOTLIN_COMPILER_ARGS = listOf(
        "-opt-in=kotlin.RequiresOptIn",
        "-opt-in=kotlin.Experimental",

        // Emit JVM type annotations in bytecode
        "-Xemit-jvm-type-annotations",

        // Enhance not null annotated type parameter's types to definitely not null types (@NotNull T => T & Any)
        "-Xenhance-type-parameter-types-to-def-not-null",

        // strict (experimental; treat as other supported nullability annotations)
        "-Xjsr305=strict",

        // When using the IR backend, run lowerings by file in N parallel threads. 0 means use a thread per processor core. Default value is 1
        "-Xbackend-threads=0",

        // Enable strict mode for some improvements in the type enhancement for loaded Java types based on nullability annotations,including freshly supported reading of the type use annotations from class files. See KT"-45671 for more details",
        "-Xtype-enhancement-improvements-strict-mode",

        // Use fast implementation on Jar FS. This may speed up compilation time, but currently it's an experimental mode
        "-Xuse-fast-jar-file-system",

        // Enable experimental context receivers
        "-Xcontext-receivers",

        // Enable additional compiler checks that might provide verbose diagnostic information for certain errors.
        "-Xextended-compiler-checks",

        // Enable incremental compilation
        "-Xenable-incremental-compilation",

        // Enable compatibility changes for generic type inference algorithm
        "-Xinference-compatibility",

        // Support inferring type arguments based on only self upper bounds of the corresponding type parameters
        "-Xself-upper-bound-inference",

        // Eliminate builder inference restrictions like allowance of returning type variables of a builder inference call
        "-Xunrestricted-builder-inference",

        "-Xnew-inference",

        // "-Xuse-k2",
        // "-Xuse-fir-extended-checkers"
    )
}
