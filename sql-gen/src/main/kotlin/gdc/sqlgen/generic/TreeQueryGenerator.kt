package gdc.sqlgen.generic

import gdc.ir.*
import gdc.ir.Field
import gdc.ir.Target
import gdc.sqlgen.utils.OperationRequestRelationGraph
import gdc.sqlgen.utils.RelationshipEdge
import org.jooq.*
import org.jooq.impl.CustomField
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType


interface TreeQueryDSL {
    fun mapFromEntries(fields: Collection<SelectField<*>>): CustomField<Record>

    fun emptyArray(): CustomField<Array<Any>>

    fun emptyObjAgg(): CustomField<Record> =
        CustomField.of("empty_object_aggregation", SQLDataType.RECORD) { ctx: Context<*> ->
            ctx.visit(
                DSL.arrayAgg(mapFromEntries(emptyList()))
            )
        }

    fun datetime(timestamp: String): CustomField<Record> =
        CustomField.of("datetime", SQLDataType.RECORD) { ctx: Context<*> ->
            ctx.visit(
                DSL.inline(timestamp)
            )
        }
}

abstract class TreeQueryGenerator(private val InnerDSL: TreeQueryDSL, private val sqlDialect: SQLDialect) :
    BaseQueryGenerator() {

    abstract val orderByDefault: Collection<OrderField<*>>?

    override fun queryRequestToSQL(request: QueryRequest) = mkQuery(QueryGenCtx.mkRootCtx(request))

    override fun forEachQueryRequestToSQL(request: QueryRequest)
            = forEachQueryRequestToSQL(request as QueryRequest, DSL.field(DSL.name(ROWS_AND_AGGREGATES)), ::mkForeachWhere)

    private fun mkForeachWhere(foreachQueryRequest: QueryRequest): Select<*> {
        return mkQuery(QueryGenCtx.mkRootCtx(foreachQueryRequest), extraRowNumberOrderBy = {
            listOf(DSL.field(DSL.name("foreach_rows", "index")))
        })
    }

    protected fun getColumnFields(fields: Map<Alias, Field>?): Map<Alias, Field.ColumnField> {
        return (fields ?: emptyMap())
            .filterValues { it is Field.ColumnField }
            .mapValues { it.value as Field.ColumnField }
    }

    protected fun getRelationFields(fields: Map<Alias, Field>?): Map<Alias, Field.RelationshipField> {
        return (fields ?: emptyMap())
            .filterValues { it is Field.RelationshipField }
            .mapValues { it.value as Field.RelationshipField }
    }

    // The context (a "stack") required during recursive query generation
    // It is passed down (and modified) as we traverse the IR tree to build the SQL query
    data class QueryGenCtx(
        // The QueryRequest that is being translated
        val request: QueryRequest,
        val currentTableAlias: String,
        // The graph of relationships between tables
        val relGraph: OperationRequestRelationGraph,
        // The edge of the relationship graph that we are currently traversing, or null if we are at the root table.
        val relEdge: RelationshipEdge?,
    ) {
        companion object {
            private const val BASE_FIELDS_SUFFIX = "_base_fields"

            private fun mkBaseTableAlias(tableName: String) = tableName + BASE_FIELDS_SUFFIX

            fun mkRootCtx(request: QueryRequest): QueryGenCtx {
                return QueryGenCtx(
                    request = request,
                    currentTableAlias = mkBaseTableAlias(request.getName().value.last()),
                    relGraph = OperationRequestRelationGraph(request),
                    relEdge = null,
                )
            }
        }

        val isRootTable: Boolean
            get() = relEdge == null

        val currentTableName: FullyQualifiedTableName
            get() = request.getName()

        fun mkChildCtx(
            nextRequest: QueryRequest,
            nextTableAlias: String,
            nextRelEdge: RelationshipEdge,
        ): QueryGenCtx {
            return this.copy(
                request = nextRequest,
                currentTableAlias = nextTableAlias,
                relEdge = nextRelEdge,
            )
        }
    }

    abstract fun mkRowsField(ctx: QueryGenCtx): SelectField<*>

    abstract fun mkAggregatesField(ctx: QueryGenCtx): SelectField<*>

    private fun mkNextQueryRequest(
        request: QueryRequest,
        relField: Field.RelationshipField
    ): QueryRequest {
        val relEdge = OperationRequestRelationGraph(request).getRelation(request.getName(), relField.relationship)
        return QueryRequest(
            query = relField.query,
            relationships = request.relationships,
            target = relEdge.entry.target
        )
    }

    abstract fun mkRowsAndAggregates(
        ctx: QueryGenCtx,
        rowFields: SelectField<*>,
        aggregatesFields: SelectField<*>
    ): SelectField<*>


    private fun mkOffsetLimit(ctx: QueryGenCtx): Condition {
        return super.mkOffsetLimit(ctx.request, DSL.field(DSL.name("rn")))
    }

    private fun mkQueryJoins(ctx: QueryGenCtx, select: SelectJoinStep<*>): List<SelectOnConditionStep<out Record>> {
        return getRelationFields(ctx.request.query.fields).map { (alias, relField) ->
            val relEdge = ctx.relGraph.getRelation(ctx.request.getName(), relField.relationship)

            val nextCtx = ctx.mkChildCtx(
                nextRequest = mkNextQueryRequest(ctx.request, relField),
                nextRelEdge = relEdge,
                nextTableAlias = alias.value
            )

            select.leftOuterJoin(
                mkQuery(nextCtx).query.asTable(DSL.name(alias.value))
            ).on(
                DSL.and(
                    nextCtx.relEdge!!.entry.column_mapping.map { (sourceCol, targetCol) ->
                        DSL.field(
                            DSL.name(ctx.currentTableName.value.plus(sourceCol.value))
                        ).eq(
                            DSL.field(
                                DSL.name(listOf(alias.value, targetCol.value))
                            )
                        )
                    }
                )
            )
        }
    }

    private fun mkQueryFrom(
        ctx: QueryGenCtx,
        extraRowNumberOrderBy: (QueryGenCtx) -> List<org.jooq.Field<*>>
    ): SelectConditionStep<Record> {
        val joinCols = mkJoinKeyFields(ctx.relEdge, ctx.currentTableName)

        return DSL.select(
            // Base columns
            DSL.table(DSL.name(ctx.currentTableName.value)).asterisk(),
            // Relationship field names from sub-query joins
            // IE: "Albums"."rows_and_aggregates" as "Albums" (from the "Albums" sub-query in an Artist -> Albums query)
            *run {
                getRelationFields(ctx.request.query.fields).map { (alias, _) ->
                    DSL.field(
                        DSL.name(alias.value).append(DSL.name(ROWS_AND_AGGREGATES))
                    ).`as`(
                        DSL.name(alias.value)
                    )
                }
            }.toTypedArray(),
            // Row number + order by
            DSL.rowNumber().over(
                DSL.partitionBy(
                    joinCols
                ).orderBy(
                    run {
                        val orderByFields =
                            translateIROrderByField(ctx.request.query.order_by, ctx.currentTableName, ctx.relGraph)

                        ((orderByFields).distinct().ifEmpty { orderByDefault } ?: emptyList())
                            .plus(extraRowNumberOrderBy(ctx))
                    }
                )
            ).`as`("rn")
        ).from(
            when (ctx.request.target) {

                is Target.FunctionTarget -> {
                    DSL.table(
                        mkUdtf(
                            sqlDialect = sqlDialect,
                            function = ctx.currentTableName,
                            functionArguments = (ctx.request.target as Target.FunctionTarget).arguments
                        )
                    ).`as`(
                        DSL.name(ctx.currentTableName.value)
                    )
                }

                else -> {
                    DSL.table(DSL.name(ctx.currentTableName.value))
                }
            }
        ).apply {
            addJoinsRequiredForOrderByFields(this, ctx.request, ctx.relGraph)
        }.apply {
            // mkQueryJoins is mutually recursive with mkQuery
            mkQueryJoins(ctx, this)
        }.where(
            getWhereConditions(ctx.request as QueryRequest, ctx.currentTableName, ctx.relGraph)
        ).apply {
            this.orderBy(
                DSL.field(DSL.name("rn"))
            )
        }
    }


    private fun mkQuery(
        ctx: QueryGenCtx,
        extraRowNumberOrderBy: (QueryGenCtx) -> List<org.jooq.Field<Any>> = { emptyList() }
    ): SelectHavingStep<Record> {

        val joinCols = mkJoinKeyFields(ctx.relEdge, FullyQualifiedTableName(ctx.currentTableAlias))
        val rowsAndAggregates = mkRowsAndAggregates(
            ctx,
            mkRowsField(ctx),
            mkAggregatesField(ctx)
        )

        return DSL.select(
            // Need to select rows and aggregates JSON, plus any parent join key fields
            joinCols + rowsAndAggregates.`as`(ROWS_AND_AGGREGATES)
        ).from(
            mkQueryFrom(ctx, extraRowNumberOrderBy).asTable(DSL.name(ctx.currentTableAlias))
        ).where(
            mkOffsetLimit(ctx)
        ).groupBy(
            joinCols.ifEmpty { listOf(DSL.noField()) }
        )

    }
}
