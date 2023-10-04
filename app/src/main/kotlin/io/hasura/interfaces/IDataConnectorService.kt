package io.hasura.interfaces

import gdc.ir.CapabilitiesResponse
import gdc.ir.MutationRequest
import gdc.ir.MutationResponse
import gdc.ir.QueryRequest
import gdc.ir.QueryResult
import gdc.ir.Schema
import gdc.ir.SchemaRequest
import io.hasura.controllers.DataConnectorResource
import io.hasura.controllers.DatasourceName
import io.hasura.models.ExplainResponse

interface IDataConnectorService {
    // The /capabilities endpoint
    fun getCapabilities(): CapabilitiesResponse

    // The /schema endpoint
    fun getSchema(
        sourceName: DatasourceName,
        config: Map<String, Any> = emptyMap(),
        schemaRequest: SchemaRequest
    ): Schema

    // The /explain endpoint
    fun explainQuery(
        sourceName: DatasourceName,
        config: Map<String, Any> = emptyMap(),
        request: QueryRequest
    ): ExplainResponse

    // The /query endpoint
    fun handleQuery(
        sourceName: DatasourceName,
        config: Map<String, Any> = emptyMap(),
        request: QueryRequest
    ): QueryResult

    // The /mutation endpoint
    fun handleMutation(
        sourceName: DatasourceName,
        config: Map<String, Any> = emptyMap(),
        request: MutationRequest
    ): MutationResponse {
        return MutationResponse()
    }

    // The /health endpoint
    // Method used in healthcheck, a SELECT 1=1 query
    fun runHealthCheckQuery(
        sourceName: DatasourceName,
        config: Map<String, Any> = emptyMap()
    ): Boolean

    fun checkForDatasetTemplate(
        templateName: String
    ): DataConnectorResource.TemplateResponse {
        return DataConnectorResource.TemplateResponse(exists = false)
    }

    fun createDatasetClone(
        cloneName: String,
        cloneFrom: DataConnectorResource.CloneFrom
    ): DataConnectorResource.CreateCloneResponse {
        return DataConnectorResource.CreateCloneResponse(config = emptyMap())
    }

    fun deleteDatasetClone(
        cloneName: String
    ): Boolean {
        return false
    }
}
