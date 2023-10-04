package gdc.sqlgen.generic

import gdc.ir.FullyQualifiedTableName
import gdc.ir.QueryRequest
import gdc.ir.Target
import gdc.sqlgen.utils.OperationRequestRelationGraph
import org.jooq.Field
import org.jooq.SQLDialect
import org.jooq.Select
import org.jooq.Table
import org.jooq.impl.DSL

object SimpleQueryGenerator : BaseQueryGenerator(){
    override fun queryRequestToSQL(
        request: QueryRequest
    ): Select<*> {
        return DSL.select(
            buildOuterStructure(
                request,
                this::buildRows
            )
        )
        .from(buildSimpleFrom(request))
    }

    private fun buildSimpleFrom(request: QueryRequest): Table<*> {
        val relGraph = OperationRequestRelationGraph(request)
        return DSL.select(
            DSL.asterisk()
        )
            .from(
                when (request.target) {

                    is Target.FunctionTarget -> {
                        DSL.table(
                            mkUdtf(
                                sqlDialect = SQLDialect.SNOWFLAKE,
                                function = request.getName(),
                                functionArguments = (request.target as Target.FunctionTarget).arguments
                            )
                        ).asTable(DSL.name(request.targetName()))
                    }

                    else ->
                        DSL.table(DSL.name(request.targetName()))


                }
            )
            .where(getWhereConditions(request, relationGraph = relGraph))
            .orderBy(translateIROrderByField(request, relationGraph = relGraph))
            .apply {
                request.query.limit?.let { limit(it) }
                request.query.offset?.let { offset(it) }
            }.asTable(request.getName().tableName)
    }

    private fun buildRows(request: QueryRequest): Field<*> {
        return DSL.coalesce(
            DSL.jsonArrayAgg(
                DSL.jsonObject(
                    request.query.fields?.map { (alias, field) ->
                        DSL.jsonEntry(
                            alias.value,
                            DSL.field(DSL.name(request.getName().tableName, (field as gdc.ir.Field.ColumnField).column))
                        )
                    }
                )
            )
                .apply{
                    request.query.order_by?.let{
                        orderBy(
                            translateIROrderByField(
                                request,
                                FullyQualifiedTableName(request.getName().tableName)
                            )
                        )
                    }
                },
            DSL.jsonArray()
        )
    }

    override fun forEachQueryRequestToSQL(request: QueryRequest): Select<*> =
        DataLoaderQueryGenerator.forEachQueryRequestToSQL(request)
}
