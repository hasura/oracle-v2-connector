package io.hasura.models

data class ExplainResponse(val lines: List<String>? = emptyList(), val query: String)
