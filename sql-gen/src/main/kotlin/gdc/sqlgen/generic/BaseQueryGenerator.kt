package gdc.sqlgen.generic

import gdc.ir.*
import gdc.ir.Target
import gdc.sqlgen.custom.OracleGenerator
import gdc.sqlgen.utils.OperationRequestRelationGraph
import gdc.sqlgen.utils.RelationshipEdge
import org.jooq.AggregateFunction
import org.jooq.CommonTableExpression
import org.jooq.Condition
import org.jooq.Field
import org.jooq.JSON
import org.jooq.Record
import org.jooq.SQLDialect
import org.jooq.Select
import org.jooq.SelectField
import org.jooq.SelectHavingStep
import org.jooq.SelectJoinStep
import org.jooq.SelectOrderByStep
import org.jooq.SelectSelectStep
import org.jooq.SortField
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

abstract class BaseQueryGenerator : BaseGenerator {

    fun handleRequest(request: QueryRequest): Select<*> {
        return when {
            request.foreach != null -> this.forEachQueryRequestToSQL(request)
            // for non-relational queries use the SimpleQueryGenerator
            // -- as there is no jOOQ dialect for Athena and as Oracle requires CLOB specification, this
            // cannot currently be used in those cases
            request.relationships.isEmpty() && request.interpolatedQueries.isNullOrEmpty()
                    && (this !is OracleGenerator) -> SimpleQueryGenerator.queryRequestToSQL(request)

            else -> this.queryRequestToSQL(request)
        }
    }

    abstract fun queryRequestToSQL(request: QueryRequest): Select<*>

    open fun mutationQueryRequestToSQL(request: QueryRequest): Select<*> {
        throw NotImplementedError("Mutation not supported for this data source")
    }

    fun getQueryColumnFields(fields: Map<Alias, gdc.ir.Field>): Map<Alias, gdc.ir.Field.ColumnField> {
        return fields
                .filterValues { it is gdc.ir.Field.ColumnField }
                .mapValues { it.value as gdc.ir.Field.ColumnField }
    }

    fun getQueryRelationFields(fields: Map<Alias, gdc.ir.Field>): Map<Alias, gdc.ir.Field.RelationshipField> {
        return fields
                .filterValues { it is gdc.ir.Field.RelationshipField }
                .mapValues { it.value as gdc.ir.Field.RelationshipField }
    }

    protected fun getAggregateFields(request: QueryRequest): Map<Alias, Aggregate> {
        return (request.query.aggregates ?: emptyMap())
    }

    protected fun mkJoinKeyFields(
            relEdge: RelationshipEdge?,
            currentTableAlias: FullyQualifiedTableName,
    ): List<Field<Any>> {
        return relEdge?.entry?.column_mapping?.map { (parentKey, childKey) ->
            DSL.field(
                    DSL.name(currentTableAlias.value + childKey.value)
            )
        } ?: emptyList()
    }

    fun mkAggregateSubquery(
            elem: OrderByElement,
            relEdge: RelationshipEdge,
            whereCondition: Condition
    ): SelectHavingStep<Record> {
        // If the target is a star-count aggregate, we need to select the special aggregate_count field
        // Otherwise, it's a regular aggregate, so our "SELECT *" will give us access to it in the other
        // parts of the query
        val orderElem = when (val target = elem.target) {
            is OrderByTarget.OrderByStarCountAggregate -> {
                listOf(DSL.count().`as`(DSL.name("aggregate_field")))
            }

            is OrderByTarget.OrderBySingleColumnAggregate -> {
                val aggregate = Aggregate.SingleColumn(target.column, target.function)
                listOf(translateIRAggregateField(aggregate).`as`(DSL.name("aggregate_field")))
            }

            else -> {
                emptyList()
            }
        }

        val joinCols = mkJoinKeyFields(relEdge, relEdge.entry.target.getTargetName())

        // Select fields that need to be present in order for the ORDER BY clause to work
        return DSL.select(
                orderElem + joinCols
        ).from(
                DSL.table(DSL.name(relEdge.entry.target.getTargetName().value))
        ).where(
                whereCondition
        ).groupBy(
                joinCols
        )
    }

    protected fun translateIROrderByField(
            request: QueryRequest,
            currentTableName: FullyQualifiedTableName = request.getName(),
            relationGraph: OperationRequestRelationGraph = OperationRequestRelationGraph(request),
    ): List<SortField<*>> {
        return translateIROrderByField(request.query.order_by, currentTableName, relationGraph)
    }

    // Translates the IR "order_by" field into a list of JOOQ SortField objects
    // This method requires that order-by target fields which reference other tables
    // have been JOIN'ed to the main query table, aliased as their Relationship table name
    // TODO: Does this break if custom table names are used?
    protected fun translateIROrderByField(
            orderBy: OrderBy?,
            currentTableName: FullyQualifiedTableName,
            relationGraph: OperationRequestRelationGraph,
    ): List<SortField<*>> {
        return orderBy?.elements?.map { elem ->
            val field = when (val target = elem.target) {
                is OrderByTarget.OrderByColumn -> {
                    if (elem.target_path.isNotEmpty()) {
                        val relEdges = relationGraph.traverseRelEdges(
                                path = elem.target_path,
                                startingTable = currentTableName
                        )
                        val targetTable = relEdges.last().entry.target.getTargetName().value
                        DSL.field(DSL.name(targetTable + target.column.value))
                    } else {
                        DSL.field(DSL.name(currentTableName.value + target.column.value))
                    }
                }


                is OrderByTarget.OrderByStarCountAggregate,
                is OrderByTarget.OrderBySingleColumnAggregate -> {
                    DSL.coalesce(
                            if (elem.target_path.isNotEmpty()) {
                                val targetTable = elem.target_path.last().value
                                DSL.field(DSL.name(targetTable + "_aggregate", "aggregate_field"))
                            } else {
                                DSL.field(DSL.name("aggregate_field"))
                            },
                            DSL.zero() as SelectField<*>
                    )
                }
            }

            when (elem.order_direction) {
                OrderDirection.ASC -> field.asc().nullsLast()
                OrderDirection.DESC -> field.desc().nullsFirst()
            }
        } ?: emptyList()
    }

    protected fun addJoinsRequiredForOrderByFields(
            select: SelectJoinStep<*>,
            request: QueryRequest,
            relationGraph: OperationRequestRelationGraph,
            sourceTableNameTransform: (FullyQualifiedTableName) -> List<String> = { it.value }
    ) {
        // Add the JOIN's required by any ORDER BY fields referencing other tables
        //
        // Make a SET to hold seen tables so we don't add the same JOIN twice
        // When we are mapping through each of the ORDER BY elements, we will
        // check if the "relEdges" for the target path have been seen before
        // If so, we don't want to add the JOIN again, because it will cause
        // a SQL error
        val seenRelations = mutableSetOf<RelationshipEdge>()
        request.query.order_by?.let { orderBy ->
            // FOR EACH ORDER BY:
            orderBy.elements.forEach { orderByElement ->
                if (orderByElement.target_path.isNotEmpty()) {
                    val relEdges = relationGraph.traverseRelEdges(
                            path = orderByElement.target_path,
                            startingTable = request.getName()
                    )
                    // FOR EACH RELATIONSHIP EDGE:
                    relEdges
                            .minus(seenRelations) // Only add JOINs for unseen relationships
                            .forEach { relEdge ->
                                seenRelations.add(relEdge)

                                val relName = RelationshipName(relEdge.relationshipName)
                                val orderByWhereCondition = orderBy.relations[relName]?.where?.let { where ->
                                    expressionToCondition(
                                            e = where,
                                            currentTableName = relEdge.entry.target.getTargetName(),
                                            relationGraph = relationGraph,
                                    )
                                } ?: DSL.noCondition()

                                when (relEdge.entry.relationship_type) {
                                    RelationshipType.OBJECT -> {
                                        select.leftJoin(
                                                DSL.table(DSL.name(relEdge.entry.target.getTargetName().value))
                                        ).on(
                                                mkSQLJoin(relEdge, sourceTableNameTransform).and(orderByWhereCondition)
                                        )
                                    }
                                    // It's an aggregate relationship, so we need to join to the aggregation subquery
                                    RelationshipType.ARRAY -> {
                                        select.leftJoin(
                                                mkAggregateSubquery(
                                                        elem = orderByElement,
                                                        relEdge = relEdge,
                                                        whereCondition = orderByWhereCondition,
                                                ).asTable(
                                                        DSL.name(relEdge.relationshipName + "_aggregate")
                                                )
                                        ).on(
                                                mkSQLJoin(
                                                        relEdge,
                                                        sourceTableNameTransform,
                                                        targetTableNameTransform = { _ -> listOf(relEdge.relationshipName + "_aggregate") }
                                                )
                                        )
                                    }
                                }
                            }
                }
            }
        }
    }

    protected fun translateIRAggregateField(field: Aggregate): AggregateFunction<*> {
        return when (field) {
            is Aggregate.StarCount -> DSL.count()
            is Aggregate.ColumnCount ->
                if (field.distinct)
                    DSL.countDistinct(DSL.field(DSL.name(field.column.value)))
                else
                    DSL.count(DSL.field(DSL.name(field.column.value)))

            is Aggregate.SingleColumn -> {
                val jooqField =
                        DSL.field(DSL.name(field.column.value), SQLDataType.NUMERIC)
                when (field.function) {
                    SingleColumnAggregateFunction.AVG -> DSL.avg(jooqField)
                    SingleColumnAggregateFunction.SUM -> DSL.sum(jooqField)
                    SingleColumnAggregateFunction.MIN -> DSL.min(jooqField)
                    SingleColumnAggregateFunction.MAX -> DSL.max(jooqField)
                    SingleColumnAggregateFunction.STDDEV_POP -> DSL.stddevPop(jooqField)
                    SingleColumnAggregateFunction.STDDEV_SAMP -> DSL.stddevSamp(jooqField)
                    SingleColumnAggregateFunction.VAR_POP -> DSL.varPop(jooqField)
                    SingleColumnAggregateFunction.VAR_SAMP -> DSL.varSamp(jooqField)
                }
            }
        }
    }

    protected fun translateIRAggregateFields(fields: Map<Alias, Aggregate>): List<Field<*>> {
        return fields.map { (alias, field) ->
            translateIRAggregateField(field).`as`(alias.value)
        }
    }

    protected fun isAggregateOnlyRequest(request: QueryRequest) =
            getQueryColumnFields(request.query.fields ?: emptyMap()).isEmpty() &&
                    getAggregateFields(request).isNotEmpty()

    protected fun buildOuterStructure(
            request: QueryRequest,
            buildRows: (request: QueryRequest) -> Field<*>,
            buildAggregates: (request: QueryRequest) -> Field<*> = ::buildAggregates
    ): Field<*> {
        return DSL.jsonObject(
                DSL.jsonEntry(
                        "rows",
                        getRows(request, buildRows)
                ),
                DSL.jsonEntry(
                        "aggregates",
                        getAggregates(request, buildAggregates)
                )
        )
    }

    private fun getRows(
            request: QueryRequest,
            buildRows: (request: QueryRequest) -> Field<*>
    ): Field<*> {
        return if (isAggregateOnlyRequest(request)) {
            DSL.inline(null as JSON?)
        } else {
            buildRows(request)
        }
    }

    private fun getAggregates(
            request: QueryRequest,
            buildAggregates: (request: QueryRequest) -> Field<*>
    ): Field<*> {
        return getAggregateFields(request).let {
            if (it.isEmpty()) {
                DSL.inline(null as JSON?)
            } else {
                buildAggregates(request)
            }
        }
    }

    protected open fun buildAggregates(request: QueryRequest): Field<*> {
        return DSL.jsonObject(
                getAggregateFields(request).map { (alias, aggregate) ->
                    DSL.jsonEntry(
                            alias.value,
                            translateIRAggregateField(aggregate)
                    )
                }
        )
    }

    protected fun mkOffsetLimit(
            request: QueryRequest,
            rowNumber: Field<Any> = DSL.field(DSL.name("rn"))
    ): Condition {
        val limit = (request.query.limit ?: 0)
        val offset = (request.query.offset ?: 0)
        return when {
            limit > 0 && offset > 0 -> {
                (rowNumber.le(DSL.inline(limit + offset)))
                        .and(rowNumber.gt(DSL.inline(offset)))
            }

            limit > 0 -> {
                (rowNumber.le(DSL.inline(limit)))
            }

            offset > 0 -> {
                (rowNumber.gt(DSL.inline(offset)))
            }

            else -> {
                DSL.noCondition()
            }
        }
    }

    protected fun getForeach(request: QueryRequest): List<Map<String, ScalarValue>> {
        return request.foreach ?: error("forEachQueryRequestToSQL called without a foreach clause")
    }

    protected fun getForeachFields(foreach: List<Map<String, ScalarValue>>): Set<String> {
        return foreach.map { it.keys }.flatten().toSet()
    }

    protected fun getForeachFields(request: QueryRequest): Set<String> {
        return getForeachFields(getForeach(request))
    }

    protected fun buildForeachCTE(request: QueryRequest, suffix: String = ""): CommonTableExpression<*> {
        val foreach = getForeach(request as QueryRequest)
        val setOfForeachFields = getForeachFields(foreach)
        return DSL
                .name(FOREACH_ROWS + suffix)
                .fields(*setOfForeachFields.toTypedArray().plus("index"))
                .`as`(
                        foreach.mapIndexed { idx, row ->
                            DSL.select(
                                    *setOfForeachFields.map { field ->
                                        DSL.inline(row[field]!!.value)
                                    }.toTypedArray(),
                                    DSL.inline(idx)
                            )
                        }.reduce { acc: SelectOrderByStep<Record>, select: SelectSelectStep<Record> ->
                            acc.unionAll(select)
                        }
                )

    }

    abstract fun forEachQueryRequestToSQL(request: QueryRequest): Select<*>

    protected fun forEachQueryRequestToSQL(
            request: QueryRequest,
            selectField: Field<*>,
            fromBuilder: (request: QueryRequest) -> Select<*>
    ): Select<*> {

        val cte = DSL.with(buildForeachCTE(request))
        val forEachQueryRequest = generateForeachRequest(request)

        return cte.select(
                selectField
        ).from(
                fromBuilder(forEachQueryRequest)
        )
    }

    private fun generateForeachRelationship(request: QueryRequest): TableRelationship {
        val setOfForeachFields = getForeachFields(request)

        return TableRelationship(
                source_table = FullyQualifiedTableName(FOREACH_ROWS),
                relationships = mapOf(
                        RelationshipName("foreach") to RelationshipEntry(
                                target = request.target,
                                relationship_type = RelationshipType.ARRAY,
                                column_mapping = setOfForeachFields.associate { field ->
                                    SourceColumnName(field) to TargetColumnName(field)
                                }
                        )
                )
        )
    }

    protected fun generateForeachRequest(request: QueryRequest): QueryRequest {
        val foreachTableRelationship = generateForeachRelationship(request)

        return QueryRequest(
                target = Target.TableTarget(
                        name = FullyQualifiedTableName(FOREACH_ROWS)
                ),
                relationships = listOf(foreachTableRelationship) + request.relationships,
                query = Query(
                        fields = mapOf(
                                Alias("query") to gdc.ir.Field.RelationshipField(
                                        relationship = foreachTableRelationship.relationships.entries.first().key,
                                        query = request.query
                                )
                        )
                ),
                foreach = request.foreach
        )
    }

    protected fun getWhereConditions(
            request: QueryRequest,
            tableName: FullyQualifiedTableName = request.getName(),
            relationGraph: OperationRequestRelationGraph = OperationRequestRelationGraph(request)
    ): Condition {
        return request.query.where?.let { where ->
            expressionToCondition(
                    e = where,
                    currentTableName = tableName,
                    relationGraph = relationGraph
            )
        } ?: DSL.noCondition()
    }

    protected fun getDefaultAggregateJsonEntries(aggregates: Map<Alias, Aggregate>?): Field<*> {
        val defaults = aggregates?.map { (alias, agg) ->
            DSL.jsonEntry(
                    DSL.inline(alias.value),
                    when (agg) {
                        is Aggregate.SingleColumn -> DSL.nullCondition()
                        is Aggregate.StarCount -> DSL.zero()
                        is Aggregate.ColumnCount -> DSL.zero()
                    }
            )
        }
        return if (defaults.isNullOrEmpty()) DSL.inline(null as JSON?) else DSL.jsonObject(defaults)
    }

    protected fun mkUdtf(
            sqlDialect: SQLDialect,
            function: FullyQualifiedTableName,
            functionArguments: List<FunctionRequestFunctionArgument>? = null,
    ): String {
        val functionFQN = function.value.joinToString(".") { "\"$it\"" }
        val functionArgsString = functionArguments.orEmpty().joinToString(", ") {
            when (it) {
                is FunctionRequestFunctionArgument.Named -> {
                    when (val arg = it.value) {
                        is ArgumentValue.Scalar -> {
                            when (arg.value_type) {
                                ScalarType.STRING -> "'${arg.value}'"
                                ScalarType.INT -> arg.value.toString()
                                ScalarType.FLOAT -> arg.value.toString()
                                ScalarType.NUMBER -> arg.value.toString()
                                ScalarType.BOOLEAN -> arg.value.toString()
                                ScalarType.DATETIME -> "'${arg.value}'"
                                ScalarType.DATETIME_WITH_TIMEZONE -> "'${arg.value}'"
                                ScalarType.DATE -> "'${arg.value}'"
                                ScalarType.TIME -> "'${arg.value}'"
                                ScalarType.TIME_WITH_TIMEZONE -> "'${arg.value}'"
                            }
                        }
                    }
                }
            }
        }

        return when (sqlDialect) {
            SQLDialect.SNOWFLAKE -> "TABLE(${functionFQN}($functionArgsString))"
            else -> "${function.tableName}($functionArgsString)"
        }
    }

    companion object {
        const val MAX_QUERY_ROWS = 2147483647
        const val FOREACH_ROWS = "foreach_rows"
        const val ROWS_AND_AGGREGATES = "rows_and_aggregates"
    }
}
