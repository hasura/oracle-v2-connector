package io.hasura.utils

import gdc.ir.SchemaRequest
import io.hasura.interfaces.SchemaCrawlerOptionsGenerator
import io.hasura.models.DatasourceConnectionInfo
import schemacrawler.schema.Catalog
import schemacrawler.tools.utility.SchemaCrawlerUtility
import us.fatehi.utility.datasource.DatabaseConnectionSources
import javax.sql.DataSource

fun mkSchemaCrawlerCatalog(
    dsConnInfo: DatasourceConnectionInfo,
    dataSource: DataSource,
    schemaRequest: SchemaRequest,
    schemaCrawlerOptionsGenerator: SchemaCrawlerOptionsGenerator
): Catalog {
    return SchemaCrawlerUtility.getCatalog(
        DatabaseConnectionSources.fromDataSource(dataSource),
        schemaCrawlerOptionsGenerator.generate(dsConnInfo, dataSource, schemaRequest)
    )
}
