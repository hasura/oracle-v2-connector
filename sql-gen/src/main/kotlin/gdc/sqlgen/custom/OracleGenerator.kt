package gdc.sqlgen.custom

import gdc.ir.QueryRequest
import gdc.sqlgen.generic.BaseJsonQueryGenerator
import gdc.sqlgen.utils.OperationRequestRelationGraph
import org.jooq.Field
import org.jooq.JSONObjectNullStep
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

object OracleGenerator : BaseJsonQueryGenerator() {

    override fun buildAggregates(request: QueryRequest): Field<*> {
        return (super.buildAggregates(request) as JSONObjectNullStep)
            .apply {
                this.returning(SQLDataType.CLOB)
            }
    }

     override fun getOuterStructure(request: QueryRequest): Field<*> {
        return super.getOuterStructure(request)
            .apply {
                    (this as JSONObjectNullStep).returning(SQLDataType.CLOB)
            }
    }

    override fun coalesceJsonObjRowsArray(request: QueryRequest) =
        DSL.coalesce(
            DSL.jsonArrayAgg(
                DSL.field(
                    DSL.name("json", "json_obj"),
                    SQLDataType.CLOB
                )
            ).apply {
                this.returning(SQLDataType.CLOB)
            },
            DSL.jsonArray().apply {
                this.returning(SQLDataType.CLOB)
            }
        )

    override fun mkJsonObj(
        request: QueryRequest,
        tableAlias: String,
        relationGraph: OperationRequestRelationGraph
    ): Field<*> {
        return super.mkJsonObj(request, tableAlias, relationGraph).apply {
            (this as JSONObjectNullStep).returning(SQLDataType.CLOB)
        }
    }
}

