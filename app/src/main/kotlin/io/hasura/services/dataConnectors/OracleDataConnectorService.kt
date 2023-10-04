package io.hasura.services.dataConnectors

import gdc.ir.ConfigSchema
import gdc.ir.QueryResult
import gdc.ir.ReleaseName
import gdc.ir.Schema
import gdc.ir.SchemaRequest
import io.hasura.controllers.DataConnectorResource
import io.hasura.controllers.DatasourceName
import io.hasura.models.DatasourceConnectionInfo
import io.hasura.models.DatasourceKind
import io.hasura.services.AgroalDataSourceService
import io.hasura.services.DataConnectorCacheManager
import io.hasura.services.DatasetProvider
import io.hasura.services.schemaGenerators.OracleSchemaGenerator
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jooq.Record
import org.jooq.Result
import org.jooq.SQLDialect
import org.jooq.conf.Settings
import org.springframework.util.StopWatch
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.UUID
import javax.sql.DataSource

const val ORACLE_DC_NAME = "oracle"

@Singleton
@Named(ORACLE_DC_NAME)
class OracleDataConnectorService @Inject constructor(
    dataSourceService: AgroalDataSourceService,
    cacheManager: DataConnectorCacheManager,
    private val datasetProvider: DatasetProvider,
    tracer: Tracer
) : BaseDataConnectorService(dataSourceService, cacheManager, tracer) {
    override val name: DatasourceName = ORACLE_DC_NAME
    override val kind: DatasourceKind = DatasourceKind.ORACLE

    override val capabilitiesResponse = super.capabilitiesResponse.copy(
        display_name = "Oracle",
        release_name = ReleaseName.GA,
        config_schemas = ConfigSchema(
            config_schema = mapOf(
                "type" to "object",
                "nullable" to false,
                "properties" to super.capabilitiesResponse.config_schemas.config_schema["properties"] as Map<*, *> + mapOf(
                    "include_schemas" to mapOf(
                        "type" to "array",
                        "nullable" to true,
                        "title" to "Include Schemas",
                        "description" to "List of schemas to include in the introspection (case sensitive)",
                        "items" to mapOf(
                            "type" to "string",
                            "nullable" to false,
                            "title" to "Schema",
                            "description" to "Schema to include in the schema crawler"
                        )
                    )
                )
            )
        )

    )

    // get singleton instance of OracleSchemaGenerator
    @Volatile private var oracleSchemaGenerator: OracleSchemaGenerator? = null
    private fun getOracleSchemaGenerator(dataSource: DataSource, connInfo: DatasourceConnectionInfo): OracleSchemaGenerator =
        oracleSchemaGenerator ?: synchronized(this) {
            oracleSchemaGenerator
                ?: OracleSchemaGenerator(
                    capabilitiesResponse.capabilities
                ).also { oracleSchemaGenerator = it }
        }

    override fun executeGetSchema(dataSource: DataSource, connInfo: DatasourceConnectionInfo, schemaRequest: SchemaRequest): Schema {
        val fullyQualifyTableNames = connInfo.config.getOrDefault("fully_qualify_all_names", true) as Boolean
        val includeSchemas = connInfo.config.getOrDefault("include_schemas", emptyList<String>()) as List<String>
        dataSource.connection.use {
            return getOracleSchemaGenerator(dataSource, connInfo)
                .getSchema(it, fullyQualifyTableNames, includeSchemas, schemaRequest)
        }
    }

    override val jooqDialect = SQLDialect.ORACLE
    override val jooqSettings = Settings().withRenderFormatted(true)

    @WithSpan
    override fun processQueryDbRows(dbRows: Result<out Record>): QueryResult {
        val json = (dbRows.getValue(0, 0).toString())
        return objectMapper.readValue(json, QueryResult::class.java)
    }

    // ////////////////////////////////////////////////////////////
    // DATASETS FUNCTIONALITY
    // ////////////////////////////////////////////////////////////

    @ConfigProperty(name = "oracle.dataset.server.connection.host")
    lateinit var oracleDatasetHost: String

    @ConfigProperty(name = "oracle.dataset.server.connection.port")
    lateinit var oracleDatasetPort: String

    @ConfigProperty(name = "oracle.dataset.server.connection.user")
    lateinit var oracleDatasetUser: String

    @ConfigProperty(name = "oracle.dataset.server.connection.password")
    lateinit var oracleDatasetPassword: String

    @ConfigProperty(name = "oracle.dataset.server.connection.instance")
    lateinit var oracleDatasetInstance: String

    override fun checkForDatasetTemplate(
        templateName: String
    ): DataConnectorResource.TemplateResponse {
        return DataConnectorResource.TemplateResponse(
            exists = datasetProvider.templateExists(
                templateName,
                ORACLE_DC_NAME
            )
        )
    }

    private fun makeOracleConnectionString(
        host: String,
        port: String,
        instance: String,
        username: String,
        password: String
    ): String {
        return "jdbc:oracle:thin:$username/$password@$host:$port:$instance"
    }

    override fun createDatasetClone(
        cloneName: String,
        cloneFrom: DataConnectorResource.CloneFrom
    ): DataConnectorResource.CreateCloneResponse {
        val templateName = cloneFrom.from

        if (!datasetProvider.templateExists(templateName, ORACLE_DC_NAME)) {
            throw Exception("The dataset template $templateName does not exist")
        }

        val connectionString = makeOracleConnectionString(
            oracleDatasetHost,
            oracleDatasetPort,
            oracleDatasetInstance,
            oracleDatasetUser,
            oracleDatasetPassword
        )
        DriverManager.getConnection(connectionString).use { connection ->
            connection.createStatement().use { statement ->
                val username = statement.enquoteIdentifier(cloneName, true)
                val password = UUID.randomUUID().toString().replace("-", "").substring(0, 30)
                statement.execute(
                    """CREATE USER $username
                       IDENTIFIED BY ${statement.enquoteIdentifier(password, true)}
                       DEFAULT TABLESPACE users
                       TEMPORARY TABLESPACE temp
                       QUOTA 10M ON users"""
                )
                statement.execute("GRANT connect to $username")
                statement.execute("GRANT resource to $username")
                statement.execute("GRANT create session to $username")
                statement.execute("GRANT create table to $username")
                statement.execute("GRANT create view to $username")

                statement.execute("ALTER SESSION SET CURRENT_SCHEMA = $username")

                datasetProvider.createSchemaInClone(connection, templateName, ORACLE_DC_NAME)
                datasetProvider.insertDataIntoClone(connection, templateName)

                return DataConnectorResource.CreateCloneResponse(
                    config = mapOf(
                        "jdbc_url" to makeOracleConnectionString(
                            oracleDatasetHost,
                            oracleDatasetPort,
                            oracleDatasetInstance,
                            username,
                            password
                        ),
                        "fully_qualify_all_names" to false
                    )
                )
            }
        }
    }

    private fun closeCloneDataSources(cloneUsername: String) {
        val cloneConnInfos = cacheManager.dataSourceCache.asMap().keys
            .filter { it.jdbcUrl.startsWith("jdbc:oracle:thin:$cloneUsername") && it.jdbcUrl.endsWith("@$oracleDatasetHost:$oracleDatasetPort:$oracleDatasetInstance") } // Yuck
        cloneConnInfos.forEach { closeDataSource(it) }
    }

    private fun killUserSessions(connection: Connection, username: String) {
        connection.prepareStatement("SELECT sid, serial# FROM v\$session WHERE username = ?").use { sessionsStmt ->
            fun getSessions(): List<String> {
                sessionsStmt.setString(1, username)
                val results = sessionsStmt.executeQuery()

                val sessions = mutableListOf<String>()
                while (results.next()) {
                    val sid = results.getInt("sid")
                    val serial = results.getInt("serial#")
                    sessions.add("$sid,$serial")
                }
                return sessions
            }

            connection.createStatement().use { killSessionStmt ->
                var stopwatch = StopWatch()
                stopwatch.start()
                var sessions: List<String>
                do {
                    if (stopwatch.totalTimeSeconds > 10) {
                        throw Exception("Timed out trying to kill user sessions to delete $username")
                    }

                    sessions = getSessions()
                    for (session in sessions) {
                        try {
                            killSessionStmt.execute("ALTER SYSTEM KILL SESSION ${killSessionStmt.enquoteLiteral(session)} IMMEDIATE")
                        } catch (e: SQLException) {
                            if (e.message?.contains("User session ID does not exist") == false) {
                                throw Exception("Failed to kill user session while deleting $username", e)
                            }
                        }
                    }
                    if (sessions.size > 0) {
                        Thread.sleep(100)
                    }
                } while (sessions.size > 0)
            }
        }
    }

    override fun deleteDatasetClone(cloneName: String): Boolean {
        val connectionString = makeOracleConnectionString(
            oracleDatasetHost,
            oracleDatasetPort,
            oracleDatasetInstance,
            oracleDatasetUser,
            oracleDatasetPassword
        )

        DriverManager.getConnection(connectionString).use { connection ->
            connection.createStatement().use { statement ->
                val username = statement.enquoteIdentifier(cloneName, true)

                closeCloneDataSources(username)
                killUserSessions(connection, cloneName)

                statement.execute("DROP USER $username CASCADE")
                return true
            }
        }
    }
}
