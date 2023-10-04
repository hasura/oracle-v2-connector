package gdc.sqlgen.generic

import gdc.ir.*
import gdc.ir.Target
import gdc.sqlgen.utils.OperationRequestRelationGraph
import gdc.sqlgen.utils.RelationshipEdge
import gdc.ir.Field as IRField
import org.jooq.*
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

object CTEQueryGenerator : BaseQueryGenerator() {
    override fun queryRequestToSQL(
        request: QueryRequest
    ): Select<*> {
        return buildCTEs(request).selectFrom(buildSelections(request).asTable("data"))
    }

    override fun forEachQueryRequestToSQL(request: QueryRequest): Select<*> {
        request as QueryRequest
        return queryRequestToSQL(
            generateForeachRequest(request)
        )
    }

    private fun buildCTEs(request: QueryRequest): WithStep {
        return DSL.with(forEachQueryLevelRecursively(request, CTEQueryGenerator::buildCTE).distinct())
    }

    private fun buildCTE(request: QueryRequest, relEdge: RelationshipEdge?): CommonTableExpression<*> {
        return if (request.getName() == FullyQualifiedTableName(FOREACH_ROWS))
            buildForeachCTE(request, "_CTE")
        else if (request.target is Target.InterpolatedTarget) {
            mkInterpolatedQueryCTE(request)
        }
        else if (request.target is Target.FunctionTarget  && relEdge == null) {
            mkUdfCTE(request)
        }
        else DSL.name(genCTEName(request.getName())).`as`(
            DSL.select(DSL.asterisk())
                .from(
                    DSL.select(DSL.table(DSL.name(request.getName().tableName)).asterisk(),
                        DSL.rowNumber().over(
                            DSL.partitionBy(
                                mkJoinKeyFields(
                                    relEdge, FullyQualifiedTableName(request.getName().tableName)
                                )
                            ).orderBy(
                                run {
                                    val orderByFields = translateIROrderByField(request)
                                    orderByFields.distinct().ifEmpty { listOf(DSL.trueCondition()) }
                                }
                            )
                        ).`as`(getRNName(request.getName()))
                    ).apply {
                        if (relEdge != null) {
                            from(DSL.name(genCTEName(relEdge.table)))
                                .innerJoin(DSL.name(relEdge.entry.target.getTargetName().value))
                                .on(mkSQLJoin
                                    (
                                        relEdge,
                                        sourceTableNameTransform = { listOf(genCTEName(it)) }
                                    )
                                )
                        } else from(DSL.name(request.getName().value))
                    }
                        .apply {
                            addJoinsRequiredForOrderByFields(
                                this as SelectJoinStep<*>,
                                request,
                                OperationRequestRelationGraph(request)
                            ) { listOf(it.tableName) }
                        }
                        .where(getWhereConditions(
                                request,
                                FullyQualifiedTableName(request.getName().tableName)
                            )
                        )
                        .asTable(request.getName().tableName)
                ).where(mkOffsetLimit(request, DSL.field(DSL.name(getRNName(request.getName())))))
        )
    }

    private fun mkUdfCTE(request: QueryRequest): CommonTableExpression<*> {
        val target = request.target as Target.FunctionTarget
        return DSL.name(genCTEName(request.getName())).`as`(
            DSL.select(DSL.asterisk())
                .from(
                    mkUdtf(
                        SQLDialect.SNOWFLAKE,
                        target.getTargetName(),
                        target.arguments
                    )
                )
                .where(getWhereConditions(request))
                .orderBy(translateIROrderByField(request))
                .apply {
                    request.query.limit?.let { limit(it) }
                    request.query.offset?.let { offset(it) }
                }
        )
    }

    fun mkInterpolatedQueryCTE(request: QueryRequest): CommonTableExpression<*> {
        if (request.interpolatedQueries == null) throw IllegalArgumentException("No interpolated queries found.")
        val iq = request.interpolatedQueries!![request.getName().tableName]
        val queryParts = iq?.items?.map{
            when(it) {
                is InterpolatedItem.InterpolatedText -> it.value
                is InterpolatedItem.InterpolatedScalar -> when(it.value_type) {
                    ScalarType.STRING -> DSL.inline(it.value, SQLDataType.VARCHAR)
                    ScalarType.INT -> DSL.inline(it.value, SQLDataType.INTEGER)
                    ScalarType.FLOAT, ScalarType.NUMBER ->
                        DSL.inline(it.value, SQLDataType.NUMERIC)
                    ScalarType.BOOLEAN -> DSL.inline(it.value, SQLDataType.BOOLEAN)
                    else -> DSL.inline(it.value)
                }
            }
        }
        return DSL.name(genCTEName(request.getName())).`as`(
            DSL.resultQuery(queryParts?.joinToString(" "))
        )
    }

    private fun <T> forEachQueryLevelRecursively(
        request: QueryRequest,
        elementFn: (request: QueryRequest, relEdge: RelationshipEdge?) -> T
    ): List<T> {

        fun recur(
            request: QueryRequest,
            relGraph: OperationRequestRelationGraph,
            relEdge: RelationshipEdge?
        ): List<T> = buildList {
            add(elementFn(request, relEdge))

            getQueryRelationFields(request.query.fields ?: emptyMap()).flatMapTo(this) {
                val relField = it.value
                val rel = relGraph.getRelation(request.getName(), relField.relationship)


                recur(
                    request = request.copy(
                        target = rel.entry.target,
                        query = relField.query
                    ),
                    relGraph = relGraph,
                    relEdge = rel
                )
            }
        }

        val relGraph = OperationRequestRelationGraph(request)
        return recur(request, relGraph, null)
    }

    private fun prefixTableName(tableName: FullyQualifiedTableName) =
        if(tableName.value.size > 1) "${tableName.value.dropLast(1).last()}_" else ""

    private fun genCTEName(tableName: FullyQualifiedTableName) = "${prefixTableName(tableName)}${tableName.value.last()}_CTE"
    private fun getRNName(tableName: FullyQualifiedTableName) = "${tableName.value.last()}_RN"

    private fun buildRows(request: QueryRequest): Field<*> {
        val isObjectTarget = isTargetOfObjRel(request)
        val agg = if (isObjectTarget) DSL::jsonArrayAggDistinct else DSL::jsonArrayAgg
        return DSL.coalesce(
            agg(
                DSL.jsonObject(
                    request.query.fields?.map { (alias, field) ->
                        when (field) {
                            is gdc.ir.Field.ColumnField ->
                                DSL.jsonEntry(
                                    alias.value,
                                    DSL.field(DSL.name(genCTEName(request.getName()), field.column))
                                )

                            is gdc.ir.Field.RelationshipField -> {
                                val relation = OperationRequestRelationGraph(request).getRelation(
                                    currentTable = request.getName(),
                                    relationName = field.relationship
                                )
                                DSL.jsonEntry(
                                    alias.value,
                                    DSL.coalesce(
                                        DSL.field(
                                            DSL.name(
                                                createAlias(
                                                    relation.entry.target.getTargetName(),
                                                    isAggOnlyRelationField(field)
                                                ),
                                                ROWS_AND_AGGREGATES
                                            )
                                        ) as Field<*>,
                                        setRelFieldDefaults(field)
                                    )
                                )
                            }
                        }
                    }
                )
            )
                .orderBy(
                    setOrderBy(request, isObjectTarget)
                ),
            DSL.jsonArray()
        )
    }

    private fun isTargetOfObjRel(request: QueryRequest): Boolean {
        return request.relationships.find {
            it.relationships.values.find { rel ->
                (rel.target.getTargetName() == request.getName() && rel.relationship_type == RelationshipType.OBJECT)
            } != null
        } != null
    }

    private fun setRelFieldDefaults(field: IRField.RelationshipField): Field<*> {
        return if (isAggOnlyRelationField(field))
            DSL.jsonObject("aggregates", setAggregateDefaults(field))
        else if(isAggRelationField(field))
            DSL.jsonObject(
                DSL.jsonEntry("rows", DSL.jsonArray()),
                DSL.jsonEntry("aggregates", setAggregateDefaults(field))
            )
        else DSL.jsonObject("rows", DSL.jsonArray())
    }

    private fun isAggRelationField(field: IRField.RelationshipField) = !field.query.aggregates.isNullOrEmpty()

    private fun isAggOnlyRelationField(field: IRField.RelationshipField) =
        field.query.fields == null && isAggRelationField(field)

    private fun setAggregateDefaults(field: IRField.RelationshipField): Field<*> =
        getDefaultAggregateJsonEntries(field.query.aggregates)

    fun setOrderBy(request: QueryRequest, isObjectTarget: Boolean): List<Field<*>> {
        return if (isObjectTarget ||
            request.target is Target.FunctionTarget ||
            request.target is Target.InterpolatedTarget) emptyList()
        else if (request.getName() == FullyQualifiedTableName(FOREACH_ROWS)) listOf(DSL.field(DSL.name(genCTEName(request.getName()),"index")) as Field<*>)
        else listOf(DSL.field(DSL.name(getRNName(request.getName()))) as Field<*>)
    }

    private fun buildSelections(request: QueryRequest): Select<*> {
        val selects = forEachQueryLevelRecursively(request, CTEQueryGenerator::buildSelect)

        // this is a non-relational query so just return the single select
        if (selects.size == 1) return selects.first().third

        val graph = OperationRequestRelationGraph(request)

        selects.forEach { (alias, table, select) ->
            graph.getOutgoingRelationsForTable(table).forEach { edge ->
                val innerSelects = selects.filter { it.second == edge.entry.target.getTargetName() }
                innerSelects.forEach { (innerAlias, innerTable, innerSelect) ->
                    run {
                        select
                            .leftJoin(
                                innerSelect.asTable(innerAlias)
                            )
                            .on(
                                mkSQLJoin(
                                    edge,
                                    sourceTableNameTransform = { listOf(genCTEName(table)) },
                                    targetTableNameTransform = { listOf(innerAlias) }
                                )
                            )
                    }
                }
            }
        }
        return selects.first().third
    }

    private fun getTargetFields(relEdge: RelationshipEdge?): List<Field<*>>? =
        relEdge?.entry?.column_mapping?.values?.map{ it.value }?.map{ DSL.field(DSL.name(genCTEName(relEdge.entry.target.getTargetName()), it)) }

    private fun buildSelect(
        request: QueryRequest,
        relEdge: RelationshipEdge? = null
    ): Triple<String, FullyQualifiedTableName, SelectJoinStep<*>> {
        val targetFields = getTargetFields(relEdge)
        val alias = createAlias(request.getName(), isAggregateOnlyRequest(request))

        return Triple(
            alias,
            request.getName(),
            DSL.select(buildOuterStructure(request, CTEQueryGenerator::buildRows).`as`(ROWS_AND_AGGREGATES))
                .apply {
                    targetFields?.let { this.select(it) }
                }
                .from(DSL.name(genCTEName(request.getName())))
                .apply {
                    targetFields?.let { groupBy(it) }
                }
        )
    }

    private fun createAlias(table: FullyQualifiedTableName, isAggregateOnly: Boolean): String {
        return "${prefixTableName(table)}${table.tableName}${if (isAggregateOnly) "_AGG" else ""}"
    }

}
