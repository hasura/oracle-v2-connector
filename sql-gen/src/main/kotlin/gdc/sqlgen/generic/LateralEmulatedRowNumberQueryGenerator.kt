package gdc.sqlgen.generic

import gdc.ir.Aggregate
import gdc.ir.Alias
import gdc.ir.Expression
import gdc.ir.FullyQualifiedTableName
import gdc.ir.OrderByTarget
import gdc.ir.OrderDirection
import gdc.ir.QueryRequest
import gdc.ir.SourceColumnName
import gdc.ir.TargetColumnName
import gdc.sqlgen.utils.OperationRequestRelationGraph
import gdc.sqlgen.utils.RelationshipEdge
import org.jooq.*
import org.jooq.impl.DSL

object LateralEmulatedRowNumberQueryGenerator : BaseQueryGenerator() {

    private const val ROW_NUM_ALIAS = "row_num"
    private const val AGGREGATE_COUNT_ALIAS = "aggregate_count"

    private fun mkCTETableAlias(fqtn: FullyQualifiedTableName): List<String> {
        val table = fqtn.value.last()
        return listOf("${table}_CTE")
    }

    private fun mkAggregateTableAlias(fqtn: FullyQualifiedTableName): List<String> {
        val table = fqtn.value.last()
        return listOf("${table}_AGG")
    }

    private fun mkTargetColAlias(targetColumnName: TargetColumnName): String {
        return targetColumnName.value.plus("_FK")
    }

    fun mkAlias(path: List<String>, alias: String) = if (path.isEmpty()) alias else "${path.joinToString("_")}_$alias"

    override fun queryRequestToSQL(request: QueryRequest): Select<*> {
        request as QueryRequest
        // Generate WITH CTE for each relationship query
        return DSL.with(
            mkCommonTableExpressions(request)
        ).select(
            mkSelections(request)
        ).from(
            DSL.name(mkCTETableAlias(request.getName()))
        ).apply {
            mkJoins(this, request)
        }
    }

    override fun forEachQueryRequestToSQL(request: QueryRequest): Select<*> =
        DataLoaderQueryGenerator.forEachQueryRequestToSQL(request)

    private fun isLiftedAggregate(request: QueryRequest?, relationshipName: String?): Boolean {
        return request?.query?.order_by?.relations?.keys?.map { k -> k.value }?.contains(relationshipName) == true
    }

    private fun mkJoins(ctx: SelectJoinStep<*>, request: QueryRequest) {
        request as QueryRequest
        // Apply LEFT JOINs
        forEachQueryLevelRecursively(
            request,
            includeRootQuery = false
        ) { innerRequest, relGraph, relEdge, path, outerRequest ->
            // Non-aggregate JOINs
            ctx.leftJoin(
                DSL.name(mkCTETableAlias(innerRequest.getName())),
            ).on(
                mkSQLJoin(
                    relEdge!!,
                    sourceTableNameTransform = { mkCTETableAlias(it) },
                    targetTableNameTransform = { mkCTETableAlias(it) }
                )
            )
            // Aggregate JOINs
            val aggregates = innerRequest.query.aggregates ?: emptyMap()
            if (aggregates.isNotEmpty()) {
                mkAggregateJoins(ctx, innerRequest.getName(), aggregates, relEdge, outerRequest)
            }
        }
    }

    private fun mkAggregateJoins(
        ctx: SelectJoinStep<*>,
        table: FullyQualifiedTableName,
        aggregates: Map<Alias, Aggregate>,
        edge: RelationshipEdge,
        outerRequest: QueryRequest? = null,
        lifted: Boolean = false
    ) {
        if (!isLiftedAggregate(outerRequest, edge.relationshipName)) {
            val primaryKeyFields = edge.entry.column_mapping.values.map { targetCol ->
                DSL.field(DSL.name(targetCol.value)).`as`(mkTargetColAlias(targetCol))
            }

            val fromTable = if (!lifted) mkCTETableAlias(table) else table.value
            ctx.leftJoin(
                // Select primary key(s) and aggregate columns
                DSL.select(primaryKeyFields + translateIRAggregateFields(aggregates))
                    .from(DSL.name(fromTable))
                    .groupBy(primaryKeyFields)
                    .asTable(DSL.name(mkAggregateTableAlias(table)))
            ).on(
                mkSQLJoin(
                    edge,
                    sourceTableNameTransform = if (!lifted) {
                        { mkCTETableAlias(it) }
                    } else {
                        { it.value }
                    },
                    targetTableNameTransform = { mkAggregateTableAlias(it) },
                    targetColNameTransform = { mkTargetColAlias(it) }
                )
            )
        }
    }

    fun getReferencedTableAndColumnMap(
        request: QueryRequest,
    ): Map<FullyQualifiedTableName, Map<SourceColumnName, TargetColumnName>> {
        val queryRequestRelationGraph = OperationRequestRelationGraph(request)
        return getQueryRelationFields(request.query.fields ?: emptyMap()).map {
            queryRequestRelationGraph.getRelation(request.getName(), it.value.relationship)
        }.associate {
            it.entry.target.getTargetName() to it.entry.column_mapping
        }
    }

    private fun mkSelections(request: QueryRequest): List<Field<*>> {
        return forEachQueryLevelRecursively(request) { innerRequest, relGraph, relEdge, path, outerRequest ->
            val fields = innerRequest.query.fields ?: emptyMap()
            val rowNumberFields = if (fields.isNotEmpty()) listOf(
                DSL.field(
                    DSL.name(mkCTETableAlias(innerRequest.getName()) + mkRnAlias(innerRequest.getName()))
                )
            ) else emptyList()  // if this is an aggregate-only query,
            // do not use row numbers as these will break grouping

            val columnFields = getQueryColumnFields(fields).map { (alias, field) ->
                val fqn = mkCTETableAlias(innerRequest.getName()) + field.column
                DSL.field(DSL.name(fqn)).`as`(mkAlias(path, alias.value))
            }

            val idFields = getReferencedTableAndColumnMap(innerRequest).flatMap { (table, columns) ->
                columns.map { (sourceCol, targetCol) ->
                    val fqn = mkCTETableAlias(innerRequest.getName()) + targetCol.value
                    DSL.field(DSL.name(fqn)).`as`(mkAlias(path, sourceCol.value))
                }
            }

            val aggregates = innerRequest.query.aggregates ?: emptyMap()
            val aggregateFields =
            // if request has no relationships but an aggregate
                // select aggregate against the main table without a join
                if (innerRequest.relationships.isEmpty()) {
                    translateIRAggregateFields(aggregates)
                } else {

                    aggregates.map { (alias, aggregate) ->
                        val fullAlias = mkAlias(path, alias.value)
                        val aggTblName = if (isLiftedAggregate(outerRequest, relEdge?.relationshipName)) {
                            mkCTETableAlias(outerRequest!!.getName())
                        } else {
                            mkAggregateTableAlias(innerRequest.getName())
                        }

                        when (aggregate) {
                            is Aggregate.ColumnCount -> DSL.field(DSL.name(aggTblName + alias.value)).`as`(fullAlias)
                            is Aggregate.SingleColumn -> DSL.field(DSL.name(aggTblName + alias.value)).`as`(fullAlias)
                            is Aggregate.StarCount -> DSL.field(DSL.name(aggTblName + AGGREGATE_COUNT_ALIAS))
                                .`as`(fullAlias)
                        }
                    }
                }
            (rowNumberFields + columnFields + idFields + aggregateFields).distinct()
        }.flatten()
    }

    private fun mkCommonTableExpressions(request: QueryRequest) =
        forEachQueryLevelRecursively(request) { innerRequest, relGraph, relEdge, path, outerRequest ->
            DSL.name(mkCTETableAlias(innerRequest.getName())).`as`(
                queryRequestToSQLInner(innerRequest, relEdge, relGraph)
            )
        }

    // Emulate LIMIT and OFFSET by using a subquery with a window function:
    // "ROW_NUMBER() OVER(PARTITION BY(...) ORDER BY(...)) AS row_num"
    private fun wrapQueryInRowNumberSubquery(
        stmt: SelectFinalStep<Record>,
        partitionBy: List<Field<*>> = emptyList(),
        orderBy: List<SortField<*>> = emptyList(),
        limit: Int = 0,
        offset: Int = 0,
        table: FullyQualifiedTableName
    ): SelectConditionStep<Record> {
        stmt.query.addSelect(
            DSL.rowNumber().over().partitionBy(partitionBy).orderBy(orderBy).`as`(ROW_NUM_ALIAS)
        )

        return DSL.select(
            DSL.asterisk(),
            DSL.field("row_num").`as`(mkRnAlias(table))
        ).from(stmt).where(
            DSL.field(DSL.name(ROW_NUM_ALIAS)).greaterThan(
                DSL.inline(offset)
            )
        ).apply {
            if (limit > 0) {
                and(
                    DSL.field(DSL.name(ROW_NUM_ALIAS)).lessOrEqual(
                        DSL.inline(offset + limit)
                    )
                )
            }
        }
    }

    private fun liftAggregateOrderBy(
        ctx: SelectJoinStep<*>,
        request: QueryRequest,
        relationGraph: OperationRequestRelationGraph
    ) {
        val fields = request.query.fields ?: emptyMap()
        request.query.order_by?.relations?.keys?.forEach { relName ->
            val relEdge = relationGraph.getRelation(request.getName(), relName)
            val rel = getQueryRelationFields(fields).values.first { relField -> relField.relationship == relName }
            mkAggregateJoins(
                ctx,
                relEdge.entry.target.getTargetName(),
                rel.query.aggregates ?: emptyMap(),
                relEdge,
                lifted = true
            )
        }
    }

    private fun queryRequestToSQLInner(
        request: QueryRequest,
        relationshipEdge: RelationshipEdge?,
        relationGraph: OperationRequestRelationGraph
    ): SelectQuery<*> {

        val orderByFields = request.query.order_by?.let { orderBy ->
            orderBy.elements.map { elem ->
                val field = when (val target = elem.target) {
                    is OrderByTarget.OrderByColumn -> DSL.field(DSL.name(target.column.value))
                    is OrderByTarget.OrderBySingleColumnAggregate -> DSL.field(DSL.name(target.column.value))
                    is OrderByTarget.OrderByStarCountAggregate -> DSL.field(DSL.name(AGGREGATE_COUNT_ALIAS))
                }
                when (elem.order_direction) {
                    OrderDirection.ASC -> field.asc()
                    OrderDirection.DESC -> field.desc()
                }
            }
        } ?: emptyList()

        val baseStmt = DSL
            .select(DSL.asterisk())
            .from(DSL.table(DSL.name(request.getName().value)))
            .apply {
                liftAggregateOrderBy(this, request, relationGraph)

                if (request.query.where != null && request.query.where != Expression.And(emptyList())) {
                    where(
                        expressionToCondition(
                            e = request.query.where!!,
                            currentTableName = request.getName(),
                            relationGraph = OperationRequestRelationGraph(request),
                        )
                    )
                }
            }
            .apply {
                orderBy(orderByFields)
            }

        val stmt = wrapQueryInRowNumberSubquery(
            stmt = baseStmt,
            limit = request.query.limit ?: 0,
            offset = request.query.offset ?: 0,
            // If we are turning this into a ROW_NUMBER() query, we need to make sure we have an ORDER BY clause
            orderBy = if (request.query.limit != null || request.query.offset != null) {
                orderByFields.ifEmpty {
                    throw IllegalArgumentException("Cannot use LIMIT or OFFSET without an ORDER BY clause")
                }
            } else {
                emptyList()
            },
            partitionBy = relationshipEdge?.entry?.column_mapping?.map { (sourceCol, targetCol) ->
                DSL.field(DSL.name(request.getName().value + sourceCol.value))
            } ?: emptyList(),
            table = request.getName()
        )

        return stmt.query
    }

    private fun <T> forEachQueryLevelRecursively(
        request: QueryRequest,
        includeRootQuery: Boolean = true,
        elementFn: (
            request: QueryRequest,
            relGraph: OperationRequestRelationGraph,
            relEdge: RelationshipEdge?,
            path: List<String>,
            outerRequest: QueryRequest?
        ) -> T
    ): List<T> {
        var rootQueryVisited = false

        fun recur(
            request: QueryRequest,
            relGraph: OperationRequestRelationGraph,
            edge: RelationshipEdge? = null,
            path: List<String> = mutableListOf(),
            outerRequest: QueryRequest? = null
        ): List<T> = buildList {
            if (includeRootQuery) {
                add(elementFn(request, relGraph, edge, path, outerRequest))
            } else {
                if (rootQueryVisited) {
                    add(elementFn(request, relGraph, edge, path, outerRequest))
                } else {
                    rootQueryVisited = true
                }
            }
            getQueryRelationFields(request.query.fields ?: emptyMap()).flatMapTo(this) {
                val relField = it.value
                val rel = relGraph.getRelation(request.getName(), relField.relationship)
                recur(
                    edge = rel,
                    relGraph = relGraph,
                    request = request.copy(
                        target = rel.entry.target,
                        query = relField.query
                    ),
                    path = path + it.key.value,
                    outerRequest = request
                )
            }
        }

        val relGraph = OperationRequestRelationGraph(request)
        return recur(request, relGraph)
    }

    fun mkRnAlias(fqtn: FullyQualifiedTableName): String {
        return fqtn.value.last() + "_RN"
    }
}
