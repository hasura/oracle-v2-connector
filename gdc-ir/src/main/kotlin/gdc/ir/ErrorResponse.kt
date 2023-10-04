package gdc.ir

data class ErrorResponse(
    val type: String = "uncaught-error",
    val message: String = "An uncaught error occurred",
    val details: Map<String, Any> = emptyMap()
)
