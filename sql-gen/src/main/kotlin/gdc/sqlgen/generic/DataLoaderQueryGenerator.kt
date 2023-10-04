package gdc.sqlgen.generic

import gdc.ir.FullyQualifiedTableName
import gdc.ir.QueryRequest
import gdc.sqlgen.utils.OperationRequestRelationGraph
import org.jooq.*
import org.jooq.impl.DSL
import gdc.ir.Field as IRField

object DataLoaderQueryGenerator : BaseQueryGenerator() {
    override fun queryRequestToSQL(
        request: QueryRequest
    ): Select<*> {
        return SimpleQueryGenerator.queryRequestToSQL(request)
    }

    override fun forEachQueryRequestToSQL(request: QueryRequest): Select<*> {
        request as QueryRequest
        return DSL.with(buildForeachCTE(request))
            .select(buildForeachStructure(request))
            .from(buildForeachFrom(request))
    }

    private fun buildForeachStructure(request: QueryRequest): Field<*> {
        return DSL.jsonObject(
            DSL.jsonEntry(
                "rows",
                DSL.jsonArrayAgg(
                    DSL.jsonObject(
                        "query",
                        DSL.coalesce(
                            DSL.field(DSL.name("query")) as Field<*>,
                            DSL.jsonObject(
                                DSL.jsonEntry("rows", DSL.jsonArray()),
                                DSL.jsonEntry("aggregates", setAggregateDefaults(request))
                            ) as Field<*>
                        )
                    )
                ).orderBy(DSL.field(DSL.name("index")))
            )
        )
    }

    private fun setAggregateDefaults(request: QueryRequest): Field<*> =
        getDefaultAggregateJsonEntries(request.query.aggregates)

    private fun buildForeachFrom(request: QueryRequest): Table<*> {
        request as QueryRequest
        val foreachFields = getForeachFields(request)
        return DSL.select(
            DSL.table(DSL.name(FOREACH_ROWS)).asterisk(),
            DSL.field(DSL.name("query", ROWS_AND_AGGREGATES)).`as`("query")
        )
            .from(DSL.name(FOREACH_ROWS))
            .leftOuterJoin(
                DSL.select(
                    *foreachFields.map { DSL.field(DSL.name(it)) }.toTypedArray(),
                    buildOuterStructure(request, DataLoaderQueryGenerator::buildForeachRow, ::buildAggregates).`as`(
                        ROWS_AND_AGGREGATES
                    )
                )
                    .apply {
                        if (request.query.limit != null || request.query.offset != null) {
                            from(buildPartitionedTable(request))
                        } else from(DSL.name(request.targetName()))
                    }
                    .where(getWhereConditions(request))
                    .groupBy(foreachFields.map { DSL.field(DSL.name(it)) })
                    .asTable("query")
            )
            .on(
                *foreachFields.map { field ->
                    DSL.field(DSL.name(FOREACH_ROWS, field))
                        .eq(DSL.field(DSL.name("query", field)))
                }.toTypedArray()
            )
            .orderBy(DSL.field(DSL.name(FOREACH_ROWS, "index")))
            .asTable()
    }

    private fun buildPartitionedTable(request: QueryRequest): Table<*> {
        request as QueryRequest
        val foreachFields = getForeachFields(getForeach(request))
        return DSL.select(
            DSL.asterisk(),
            DSL.rowNumber()
                .over(
                    DSL.partitionBy(
                        *foreachFields.map { DSL.field(DSL.name(it)) }.toTypedArray()
                    ).orderBy(translateIROrderByField(request))
                ).`as`("rn")
        )
            .from(DSL.name(request.targetName()))
            .qualify(mkOffsetLimit(request))
            .asTable(request.targetName())
    }

    private fun buildForeachRow(request: QueryRequest): Field<*> {
        request as QueryRequest
        return DSL.jsonArrayAgg(
            DSL.jsonObject(
                request.query.fields?.map { (alias, field) ->
                    DSL.jsonEntry(
                        alias.value,
                        DSL.field(
                            DSL.name(
                                request.targetName(),
                                (field as IRField.ColumnField).column
                            )
                        )
                    )
                }
            )
        ).orderBy(
            translateIROrderByField(
                request,
                FullyQualifiedTableName(request.targetName())
            )
        )
    }

}
