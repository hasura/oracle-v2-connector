package gdc.interfaces

import gdc.ir.CapabilitiesResponse
import gdc.ir.Query
import gdc.ir.Schema

interface IDataConnectorAdapter<T_HTTP_REQ, T_HEALTH_CHECK_RESPONSE> {
    fun healthCheck(): T_HEALTH_CHECK_RESPONSE
    fun getCapabilities(httpRequest: T_HTTP_REQ): CapabilitiesResponse
    fun getSchema(httpRequest: T_HTTP_REQ): Schema
    fun handleQuery(httpRequest: T_HTTP_REQ, query: Query): List<Map<String, Any?>>
}
