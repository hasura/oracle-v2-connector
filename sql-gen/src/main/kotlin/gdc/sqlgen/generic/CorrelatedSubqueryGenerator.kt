package gdc.sqlgen.generic

import gdc.ir.FullyQualifiedTableName
import gdc.ir.QueryRequest
import gdc.ir.Target
import gdc.sqlgen.utils.OperationRequestRelationGraph
import gdc.sqlgen.utils.RelationshipEdge
import org.jooq.*
import org.jooq.impl.CustomField
import org.jooq.impl.DSL


interface CorrelatedSubqueryDSL {
    fun jsonArrayAgg(value: Field<*>?, orderBy: QueryPart?, limit: Int?, offset: Int?): CustomField<Record>
}

abstract class CorrelatedSubqueryGenerator(private val CorrSubDSL: CorrelatedSubqueryDSL) : BaseQueryGenerator() {

    private fun handleOrderBy(
        request: QueryRequest,
        relationGraph: OperationRequestRelationGraph
    ): QueryPart {
        val orderBy = translateIROrderByField(
            orderBy = request.query.order_by,
            currentTableName = request.target.getTargetName(),
            relationGraph = relationGraph,
        )

        if (orderBy.isEmpty()) return DSL.field("")

        return DSL.orderBy(orderBy)
    }

    override fun forEachQueryRequestToSQL(request: QueryRequest) =
        forEachQueryRequestToSQL(request as QueryRequest, DSL.field(DSL.name("data")), ::queryRequestToSQL)


    override fun queryRequestToSQL(request: QueryRequest): SelectQuery<*> = run {
        request as QueryRequest
        val relationGraph = OperationRequestRelationGraph(request)
        val tableAlias = getTableAlias(request, relationGraph)

        DSL.select(
            buildOuterStructure(request, ::buildRows, this::buildAggregates).`as`("data")
        )
        .from(
            DSL.table(DSL.name(request.target.getTargetName().value)).`as`(DSL.name(tableAlias))
        )
        .apply {
            addJoinsRequiredForOrderByFields(this, request, relationGraph)
        }
        .where(getWhereConditions(request, FullyQualifiedTableName(tableAlias), relationGraph))
        .query
    }

    private fun getRelFields(request: QueryRequest, relationGraph: OperationRequestRelationGraph): List<RelationshipEdge> {
        request as QueryRequest
        return request.query.fields?.filter {
            it.value is gdc.ir.Field.RelationshipField
        }?.map { (alias, field) ->
            relationGraph.getRelation(
                currentTable = request.target.getTargetName(),
                relationName = (field as gdc.ir.Field.RelationshipField).relationship
            )
        } ?: emptyList()
    }

    private fun containsSelfJoin(relEdges: List<RelationshipEdge>): Boolean {
        return relEdges.find {
            it.table.tableName == it.entry.target.getTargetName().tableName
        } != null
    }

    private fun containsSelfJoin(request: QueryRequest, relationGraph: OperationRequestRelationGraph): Boolean {
        return containsSelfJoin(getRelFields(request, relationGraph))
    }

    private fun genTableAlias(tableName: String): String {
        return tableName + "_" + tableName.hashCode()
    }

    private fun getTableAlias(request: QueryRequest, relationGraph: OperationRequestRelationGraph): String {
        return  if (containsSelfJoin(request, relationGraph))
                    genTableAlias(request.getName().tableName)
                else request.getName().tableName
    }

    private fun buildRows(request: QueryRequest): Field<*> {
        request as QueryRequest
        val relationGraph = OperationRequestRelationGraph(request)
        val tableAlias = getTableAlias(request, relationGraph)
        return DSL.coalesce(
            CorrSubDSL.jsonArrayAgg(
                DSL.jsonObject(
                    request.query.fields?.map { (alias, field) ->
                        when (field) {

                            is gdc.ir.Field.RelationshipField -> {
                                val relation = relationGraph.getRelation(
                                    currentTable = request.target.getTargetName(),
                                    relationName = field.relationship
                                )

                                DSL.jsonEntry(
                                    alias.value,
                                    queryRequestToSQL(
                                        QueryRequest(
                                            target = relation.entry.target,
                                            relationships = request.relationships,
                                            query = field.query
                                        )
                                    ).apply {
                                        addConditions(
                                            DSL.and(
                                                relation.entry.column_mapping.map { (source, target) ->
                                                    DSL.field(DSL.name(tableAlias, source.value))
                                                        .eq(DSL.field(DSL.name(relation.entry.target.getTargetName().value + target.value)))
                                                }
                                            )
                                        )
                                    }
                                )
                            }

                            is gdc.ir.Field.ColumnField -> DSL.jsonEntry(
                                alias.value,
                                DSL.field(DSL.name(tableAlias, field.column))
                            )
                        }
                    }
                ),
                handleOrderBy(request, relationGraph),
                request.query.limit ?: MAX_QUERY_ROWS,
                request.query.offset ?: 0),
            DSL.jsonArray()
        )
    }

    override fun buildAggregates(request: QueryRequest): Field<*> {
        request as QueryRequest
        val relationGraph = OperationRequestRelationGraph(request)
        return getAggregateFields(request).let {
            DSL.jsonObject(
                it.map { (alias, aggregate) ->
                    DSL.jsonEntry(
                        alias.value,
                        if (isAggregateOnlyRequest(request)
                            && relationGraph.tableRelationships.isEmpty()
                            && request.query.where == null
                        ) {
                            DSL.select(
                                translateIRAggregateField(aggregate)
                            )
                                .from(
                                    DSL.select(
                                        DSL.asterisk()
                                    )
                                        .from(request.targetName())
                                        .limit(
                                            request.query.offset ?: 0,
                                            request.query.limit ?: MAX_QUERY_ROWS
                                        )
                                )
                        } else translateIRAggregateField(aggregate)
                    )
                }
            )
        }
    }
}
