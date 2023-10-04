package io.hasura.models

import io.hasura.controllers.DatasourceName
import io.hasura.controllers.JDBCUrl

data class DatasourceConnectionInfo(
    val name: DatasourceName,
    val jdbcUrl: JDBCUrl,
    val config: Map<String, Any> = emptyMap()
)
