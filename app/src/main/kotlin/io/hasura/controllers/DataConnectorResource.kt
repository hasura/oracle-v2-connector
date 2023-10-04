package io.hasura.controllers

import gdc.ir.CapabilitiesResponse
import gdc.ir.MutationRequest
import gdc.ir.MutationResponse
import gdc.ir.QueryRequest
import gdc.ir.QueryResult
import gdc.ir.Schema
import gdc.ir.SchemaRequest
import io.hasura.interfaces.IDataConnectorService
import io.hasura.models.ConfigHeader
import io.hasura.models.ExplainResponse
import io.opentelemetry.instrumentation.annotations.SpanAttribute
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.enterprise.inject.Instance
import jakarta.enterprise.inject.literal.NamedLiteral
import jakarta.inject.Inject
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.WebApplicationException
import jakarta.ws.rs.core.Context
import jakarta.ws.rs.core.HttpHeaders
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.resteasy.reactive.RestHeader
import org.jboss.resteasy.reactive.RestPath
import kotlin.Any
import jakarta.enterprise.inject.Any as JavaAny

typealias DatasourceName = String
typealias JDBCUrl = String

@Path("/api/v1/{datasourceName}")
class DataConnectorResource {

    @ConfigProperty(name = "datasets.enabled")
    var datasetsEnabled: Boolean = false

    @Inject
    @JavaAny
    private lateinit var DataConnectors: Instance<IDataConnectorService>

    @GET
    @Path("/health")
    @WithSpan
    fun health(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @Context headers: HttpHeaders
    ): Response {
        val configHeaderString = headers.getHeaderString("X-Hasura-DataConnector-Config")
        val sourceName = headers.getHeaderString("X-Hasura-DataConnector-SourceName")

        // No config/source name, just checking if the service is up
        if (configHeaderString == null && sourceName == null) {
            return Response
                .status(Response.Status.NO_CONTENT)
                .build()
        }

        // "If the endpoint is sent the X-Hasura-DataConnector-Config and X-Hasura-DataConnector-SourceName headers,
        // then the agent is expected to check that it can successfully talk to whatever data source is being specified by those headers."
        val configHeader = ConfigHeader.fromString(configHeaderString)
        val config = processHeaders(datasourceName, configHeader)
        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()

        val canConnectToDB = dataConnectorService.runHealthCheckQuery(datasourceName, config)
        if (canConnectToDB) {
            return Response
                .status(Response.Status.NO_CONTENT)
                .build()
        } else {
            throw RuntimeException("Unable to connect to DB")
        }
    }

    @GET
    @Path("/capabilities")
    @WithSpan
    fun getCapabilities(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String
    ): CapabilitiesResponse {
        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        return dataConnectorService.getCapabilities()
    }

    @POST
    @Path("/schema")
    @WithSpan
    @Consumes(MediaType.APPLICATION_JSON)
    fun getSchemaWithRequest(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @RestHeader("X-Hasura-DataConnector-Config") configHeader: ConfigHeader,
        @RestHeader("X-Hasura-DataConnector-SourceName") sourceName: String,
        schemaRequest: SchemaRequest?
    ): Schema {
        val config = processHeaders(datasourceName, configHeader)
        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        return dataConnectorService.getSchema(sourceName, config, schemaRequest ?: SchemaRequest())
    }

    @POST
    @Path("/query")
    @WithSpan
    fun handleQuery(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @RestHeader("X-Hasura-DataConnector-Config") configHeader: ConfigHeader,
        @RestHeader("X-Hasura-DataConnector-SourceName") sourceName: String,
        query: QueryRequest
    ): QueryResult {
        val config = processHeaders(datasourceName, configHeader)
        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        return dataConnectorService.handleQuery(sourceName, config, query)
    }

    @POST
    @Path("/mutation")
    @WithSpan
    fun handleMutation(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @RestHeader("X-Hasura-DataConnector-Config") configHeader: ConfigHeader,
        @RestHeader("X-Hasura-DataConnector-SourceName") sourceName: String,
        mutation: MutationRequest
    ): MutationResponse {
        val config = processHeaders(datasourceName, configHeader)
        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        return dataConnectorService.handleMutation(sourceName, config, mutation)
    }

    @POST
    @Path("/explain")
    @WithSpan
    fun explainQuery(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @RestHeader("X-Hasura-DataConnector-Config") configHeader: ConfigHeader,
        @RestHeader("X-Hasura-DataConnector-SourceName") sourceName: String,
        query: QueryRequest
    ): ExplainResponse {
        val config = processHeaders(datasourceName, configHeader)
        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        return dataConnectorService.explainQuery(sourceName, config, query)
    }

    private fun processHeaders(pathSourceName: String, configHeader: ConfigHeader): Map<String, Any> {
        verifyDatasource(pathSourceName, configHeader.value.getOrDefault("jdbc_url", "") as String)
        return customizeJDBCConfig(pathSourceName, configHeader)
    }

    private fun verifyDatasource(pathSourceName: String, config: String) {
        if (!config.contains(pathSourceName, true)) {
            throw BadRequestException("There was a mismatch between the path and the headers")
        }
    }

    private fun customizeJDBCConfig(pathSourceName: String, configHeader: ConfigHeader): Map<String, Any> {
        fun addJDBCParams(params: String): Map<String, Any> {
            return mapOf("jdbc_url" to "${configHeader.value["jdbc_url"]}&$params")
        }

        return when (pathSourceName) {
            "snowflake" -> configHeader.value + addJDBCParams("application=Hasura_HGE")
            "mysql" -> configHeader.value + addJDBCParams("allowMultiQueries=true")
            "mariadb" -> configHeader.value + addJDBCParams("allowMultiQueries=true")
            else -> configHeader.value
        }
    }

    // GET /datasets/templates/:template_name -> {"exists": true|false}
    // POST /datasets/clones/:clone_name {"from": template_name} -> {"config": {...}}
    // DELETE /datasets/clones/:clone_name -> {"message": "success"}

    data class TemplateResponse(
        val exists: Boolean
    )

    @GET
    @Path("/datasets/templates/{templateName}")
    @WithSpan
    fun getTemplate(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @RestPath templateName: String
    ): TemplateResponse {
        if (!datasetsEnabled) throw WebApplicationException(404)

        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        return dataConnectorService.checkForDatasetTemplate(templateName)
    }

    data class CloneFrom(
        val from: String
    )

    data class CreateCloneResponse(
        val config: Map<String, Any>
    )

    @POST
    @Path("/datasets/clones/{cloneName}")
    @WithSpan
    fun createClone(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @RestPath cloneName: String,
        cloneFrom: CloneFrom
    ): CreateCloneResponse {
        if (!datasetsEnabled) throw WebApplicationException(404)

        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        return dataConnectorService.createDatasetClone(cloneName, cloneFrom)
    }

    @DELETE
    @Path("/datasets/clones/{cloneName}")
    @WithSpan
    fun deleteClone(
        @RestPath @SpanAttribute("datasourceName")
        datasourceName: String,
        @RestPath cloneName: String
    ): Response {
        if (!datasetsEnabled) throw WebApplicationException(404)

        val dataConnectorService = DataConnectors.select(NamedLiteral.of(datasourceName)).get()
        val wasSuccessful = dataConnectorService.deleteDatasetClone(cloneName)
        // Return "{message: "success"}"
        return Response
            .status(Response.Status.OK)
            .entity(mapOf("message" to if (wasSuccessful) "success" else "failure"))
            .build()
    }
}
