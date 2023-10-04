package io.hasura.interfaces

import io.hasura.models.DatasourceConnectionInfo
import schemacrawler.schema.Catalog
import javax.sql.DataSource

fun interface ICatalogProvider {
    fun getCatalog(dsConnInfo: DatasourceConnectionInfo, dataSource: DataSource): Catalog
}
