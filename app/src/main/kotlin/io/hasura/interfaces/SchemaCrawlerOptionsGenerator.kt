package io.hasura.interfaces

import gdc.ir.SchemaRequest
import io.hasura.models.DatasourceConnectionInfo
import schemacrawler.schemacrawler.SchemaCrawlerOptions
import javax.sql.DataSource

/**
 * Generates the datasource-specific options needed to produce a SchemaCrawler catalog.
 */
fun interface SchemaCrawlerOptionsGenerator {
    fun generate(
        dsConnInfo: DatasourceConnectionInfo,
        dataSource: DataSource,
        schemaRequest: SchemaRequest
    ): SchemaCrawlerOptions
}
