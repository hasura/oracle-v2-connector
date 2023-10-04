package io.hasura.services

import com.github.benmanes.caffeine.cache.Caffeine
import io.hasura.models.DatasourceConnectionInfo
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics
import jakarta.inject.Singleton
import javax.sql.DataSource

@Singleton
class DataConnectorCacheManager {
    private val meterRegistry = Metrics.globalRegistry

    val dataSourceCache = Caffeine.newBuilder().recordStats()
        .build<DatasourceConnectionInfo, DataSource>()
        .also {
            CaffeineCacheMetrics.monitor(meterRegistry, it, "dataconnector.datasource.cache")
        }
}
