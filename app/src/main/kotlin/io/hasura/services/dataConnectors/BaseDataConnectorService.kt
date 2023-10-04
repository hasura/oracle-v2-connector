package io.hasura.services.dataConnectors

import com.fasterxml.jackson.databind.ObjectMapper
import gdc.ir.CapabilitiesResponse
import gdc.ir.MutationRequest
import gdc.ir.MutationResponse
import gdc.ir.QueryRequest
import gdc.ir.QueryResult
import gdc.ir.Schema
import gdc.ir.SchemaRequest
import gdc.sqlgen.SqlGeneratorFactory
import io.hasura.JDBC_DATASOURCE_COMMON_CAPABILITY_RESPONSE
import io.hasura.controllers.DatasourceName
import io.hasura.controllers.JDBCUrl
import io.hasura.interfaces.IDataConnectorService
import io.hasura.models.DatasourceConnectionInfo
import io.hasura.models.DatasourceKind
import io.hasura.models.ExplainResponse
import io.hasura.services.AgroalDataSourceService
import io.hasura.services.DataConnectorCacheManager
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.enterprise.inject.Produces
import jakarta.inject.Inject
import org.eclipse.microprofile.faulttolerance.Retry
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Result
import org.jooq.ResultQuery
import org.jooq.SQLDialect
import org.jooq.conf.Settings
import org.jooq.conf.StatementType
import org.jooq.impl.DSL
import java.time.temporal.ChronoUnit
import javax.sql.DataSource

abstract class BaseDataConnectorService(
    protected val dataSourceService: AgroalDataSourceService,
    protected val cacheManager: DataConnectorCacheManager,
    private val tracer: Tracer
) : IDataConnectorService {

    @Inject
    protected lateinit var objectMapper: ObjectMapper

    abstract val name: DatasourceName
    abstract val kind: DatasourceKind
    abstract val jooqDialect: SQLDialect
    abstract val jooqSettings: Settings

    open val capabilitiesResponse = JDBC_DATASOURCE_COMMON_CAPABILITY_RESPONSE

    val commonDSLContextSettings: Settings =
        Settings()
            .withRenderFormatted(true)
            .withStatementType(StatementType.STATIC_STATEMENT)

    @WithSpan
    override fun getCapabilities(): CapabilitiesResponse {
        return this.capabilitiesResponse
    }

    protected abstract fun executeGetSchema(dataSource: DataSource, connInfo: DatasourceConnectionInfo, schemaRequest: SchemaRequest): Schema

    @WithSpan
    fun buildQuery(queryRequest: QueryRequest): ResultQuery<*> =
        SqlGeneratorFactory.getSqlGenerator(jooqDialect).handleRequest(queryRequest)

    @WithSpan
    open fun processQueryDbRows(dbRows: Result<out Record>): QueryResult {
        val json = (dbRows.getValue(0, 0).toString())
        return objectMapper.readValue(json, QueryResult::class.java)
    }

    @WithSpan
    fun executeQuery(query: ResultQuery<*>, ctx: DSLContext): QueryResult {
        val rows = executeDbQuery(query, ctx)
        return processQueryDbRows(rows)
    }

    // Retry configuration overrideable in properties using the format:
    // io.hasura.services.dataConnectors.BaseDataConnectorService/executeDbQuery/Retry/maxRetries = X
    @WithSpan
    @Retry(maxRetries = 5, delay = 1, delayUnit = ChronoUnit.SECONDS)
    open fun executeDbQuery(query: ResultQuery<*>, ctx: DSLContext): Result<out Record> {
        return ctx.fetch(query)
    }

    protected open fun executeAndSerializeMutation(
        request: MutationRequest,
        ctx: DSLContext
    ): MutationResponse {
        val results = SqlGeneratorFactory.getMutationTranslator(jooqDialect).translate(
            request,
            ctx,
            SqlGeneratorFactory.getSqlGenerator(jooqDialect)::mutationQueryRequestToSQL
        )
        return MutationResponse(results)
    }

    private fun mkDatasourceConnectionInfo(
        sourceName: DatasourceName,
        config: Map<String, Any>
    ): DatasourceConnectionInfo {
        return DatasourceConnectionInfo(
            sourceName,
            config["jdbc_url"] as JDBCUrl,
            config.filterKeys { it != "jdbc_url" }
        )
    }

    @WithSpan
    open fun mkDSLCtx(sourceName: DatasourceName, config: Map<String, Any>): DSLContext {
        val connInfo = mkDatasourceConnectionInfo(sourceName, config)
        val datasource = cacheManager.dataSourceCache.get(connInfo) {
            dataSourceService.createDataSourceFromConnInfo(connInfo, it.config)
        }
        return DSL.using(datasource, jooqDialect, jooqSettings)
    }

    @WithSpan
    override fun getSchema(sourceName: DatasourceName, config: Map<String, Any>, schemaRequest: SchemaRequest): Schema {
        val connInfo = mkDatasourceConnectionInfo(sourceName, config)
        val dataSource = cacheManager.dataSourceCache.get(connInfo) {
            dataSourceService.createDataSourceFromConnInfo(connInfo, it.config)
        }
        return executeGetSchema(dataSource, connInfo, schemaRequest)
    }

    fun closeDataSource(connInfo: DatasourceConnectionInfo) {
        val dataSource = cacheManager.dataSourceCache.getIfPresent(connInfo)
        (dataSource as? AutoCloseable)?.close()
        cacheManager.dataSourceCache.invalidate(connInfo)
    }

    @WithSpan
    override fun explainQuery(
        sourceName: DatasourceName,
        config: Map<String, Any>,
        request: QueryRequest
    ): ExplainResponse {
        val dslCtx = mkDSLCtx(sourceName, config)
        val query = buildQuery(request)
        val explain = dslCtx.explain(query)
        return ExplainResponse(query = query.sql, lines = listOf(explain.plan()))
    }

    @WithSpan
    override fun handleQuery(
        sourceName: DatasourceName,
        config: Map<String, Any>,
        request: QueryRequest
    ): QueryResult {
        val dslCtx = mkDSLCtx(sourceName, config)
        val query = buildQuery(request)
        return executeQuery(query, dslCtx)
    }

    @WithSpan
    override fun handleMutation(
        sourceName: DatasourceName,
        config: Map<String, Any>,
        request: MutationRequest
    ): MutationResponse {
        val dslCtx = mkDSLCtx(sourceName, config)
        return executeAndSerializeMutation(request, dslCtx)
    }

    @WithSpan
    override fun runHealthCheckQuery(sourceName: DatasourceName, config: Map<String, Any>): Boolean {
        val dslCtx = mkDSLCtx(sourceName, config)
        val query = dslCtx.selectOne()
        return try {
            dslCtx.fetch(query)
            true
        } catch (e: Exception) {
            false
        }
    }

    @Produces
    fun createDataConnectorService(): IDataConnectorService {
        return this
    }
}
