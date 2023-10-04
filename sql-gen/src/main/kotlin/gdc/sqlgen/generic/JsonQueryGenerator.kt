package gdc.sqlgen.generic

import gdc.ir.*
import gdc.ir.Target
import gdc.sqlgen.utils.OperationRequestRelationGraph
import gdc.sqlgen.utils.RelationshipEdge
import gdc.ir.Field as IRField
import org.jooq.*
import org.jooq.Table
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID


abstract class BaseJsonQueryGenerator : BaseQueryGenerator() {
    /**
     * Generates a JOOQ query that returns a recursive JSON object with the following structure:
     * {
     *  "rows": [{...}, {...}, ...],
     *  "aggregates": {...}
     * }
     *
     * Where the "rows" array contains the results of the query, and the "aggregates" object contains the results of the
     * aggregate functions.
     *
     * If there are no aggregate functions, the "aggregates" object is null.
     * If there are no fields selected, but there are aggregate functions, the "rows" array is null.
     */
    override fun queryRequestToSQL(request: QueryRequest): Select<*> {
      return internalQueryRequestToSQL(request)
    }
    private fun internalQueryRequestToSQL(
        request: QueryRequest,
        previousRelationshipEdge: RelationshipEdge? = null,
        previousTableAlias: String? = null
    ): Select<*> {
        val relationGraph = OperationRequestRelationGraph(request)
        return DSL.select(
           getOuterStructure(request).`as`("data")
        ).from(
            // "LATERAL" here so that LIMIT/OFFSET work properly (LATERAL semantics are similar to a "for-each" loop)
            // i.e. so that they apply per-query-depth, not per-all-query-results
            // Otherwise LIMIT 2 would mean "A limit of 2 total results" instead of "A limit of 2 results per entity at each depth"
            mkJsonQueryLateralSubquery(
                request,
                relationGraph,
                previousRelationshipEdge,
                previousTableAlias
            )
        )
    }

    protected open fun getOuterStructure(request: QueryRequest): Field<*> {
        return buildOuterStructure(
            request,
            ::coalesceJsonObjRowsArray,
            this::buildAggregates
        )
    }

    override fun mutationQueryRequestToSQL(request: QueryRequest) =
        mutationQueryRequestToSQL(request, null)

    /**
     * SELECT
     *  coalesce(json_array_agg(json.json_obj), json_array())
     * FROM LATERAL (
     *   ...
     * ) AS json
     *
     * Returns a JSON array of JSON objects, where each JSON object is a row of the query
     * Example:
     * [
     *   {
     *     "col_name": "col_value",
     *     "rel_name": [
     *       {
     *         "rows": [ {...}, {...}, ... ],
     *         "aggregates": {...}
     *       }
     *     ]
     * ]
     */
    fun mutationQueryRequestToSQL(
        request: QueryRequest,
        previousRelationshipEdge: RelationshipEdge? = null
    ): Select<*> {
        val relationGraph = OperationRequestRelationGraph(request)
        return DSL.select(
            coalesceJsonObjRowsArray(request)
        ).from(
            mkJsonQueryLateralSubquery(request, relationGraph, previousRelationshipEdge)
        )
    }

    override fun forEachQueryRequestToSQL(request: QueryRequest)
        = forEachQueryRequestToSQL(request as QueryRequest, DSL.field(DSL.name("data")), ::queryRequestToSQL)

    /**
     * coalesce(json_array_agg(json.json_obj), json_array())
     */
    protected open fun coalesceJsonObjRowsArray(request: QueryRequest) =
        DSL.coalesce(
            DSL.jsonArrayAgg(
                DSL.field(
                    DSL.name("json", "json_obj"),
                        SQLDataType.JSON
                )
            ),
            DSL.jsonArray()
        )

    private fun addJoinsRequiredForOrderByFields(
        select: SelectJoinStep<Record>,
        request: QueryRequest,
        relationGraph: OperationRequestRelationGraph,
        tableAlias: String,
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
                        .forEachIndexed { idx, relEdge ->
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
                                        mkSQLJoin(
                                            relEdge,
                                            sourceTableNameTransform = {
                                                // If this is the first index, we want to use the table alias
                                                // Otherwise, we want to use the regular table name
                                                if (idx == 0) {
                                                    listOf(tableAlias)
                                                } else {
                                                    it.value
                                                }
                                            }
                                        )
                                    ).and(
                                        orderByWhereCondition
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
                                            targetTableNameTransform = { _ -> listOf(relEdge.relationshipName + "_aggregate") },
                                            sourceTableNameTransform = {
                                                if (idx == 0) {
                                                    listOf(tableAlias)
                                                } else {
                                                    it.value
                                                }
                                            }
                                        )
                                    )
                                }
                            }
                        }
                }
            }
        }
    }

    // Fully-qualified org.jooq name used to distinguish from java.sql

    private fun columnFieldTypeTojOOQSQLDataType(field: IRField.ColumnField): org.jooq.DataType<out Any> {
        return when (field.column_type.uppercase()) {
            ScalarType.INT.name -> SQLDataType.INTEGER
            ScalarType.FLOAT.name -> SQLDataType.FLOAT
            ScalarType.NUMBER.name -> SQLDataType.FLOAT
            ScalarType.STRING.name -> SQLDataType.VARCHAR
            ScalarType.BOOLEAN.name -> SQLDataType.BOOLEAN
            ScalarType.DATETIME.name -> SQLDataType.TIMESTAMP
            ScalarType.DATE.name -> SQLDataType.DATE
            ScalarType.TIME.name -> SQLDataType.TIME
            ScalarType.DATETIME_WITH_TIMEZONE.name -> SQLDataType.TIMESTAMPWITHTIMEZONE
            ScalarType.TIME_WITH_TIMEZONE.name -> SQLDataType.TIMEWITHTIMEZONE
            else -> throw IllegalArgumentException("Unknown scalar type: ${field.column_type} for column: ${field.column}")
        }
    }

    /**
     * LATERAL (
     *  SELECT
     *    <tableAlias>.*,
     *    json_obj(
     *      json_entry('col_name', <tableAlias>.<col_name>),
     *      json_entry('rel_name', (queryRequestToSQL(relationQueryRequest)))
     *    ) AS json_obj
     *    FROM <table> AS <tableAlias>
     *    JOIN <joinClauses required for ORDER BY>
     *    WHERE <whereClauses>
     *      AND <whereClauses required for JOIN to previous table in recursive query>
     *    ORDER BY <orderByClauses>
     *    LIMIT <limit>
     *    OFFSET <offset>
     * ) AS json
     */
    private fun mkJsonQueryLateralSubquery(
        request: QueryRequest,
        relationGraph: OperationRequestRelationGraph,
        previousRelationshipEdge: RelationshipEdge?,
        previousTableAlias: String? = null
    ): Table<Record> {
        val tableAlias = request.getName().tableName + "_" + UUID.randomUUID().toString().replace("-", "")
        val relationGraphWithTableAlias = OperationRequestRelationGraph(request.relationships
            .map {
                if (it is TableRelationship && it.source_table == request.getName()) {
                    it.copy(source_table = FullyQualifiedTableName(tableAlias))
                } else {
                    it
                }
            })

        return DSL.lateral(
            DSL.select(
                DSL.table(DSL.name(tableAlias)).asterisk(),
                mkJsonObj(request, tableAlias, relationGraph).`as`(
                    DSL.name("json_obj")
                )
            ).from(
                when (request.target) {
                    is Target.FunctionTarget -> {
                        DSL.table(
                            DSL.function(DSL.name(request.getName().value), SQLDataType.OTHER).`as`(DSL.name(tableAlias))
                        )
                    }
                    else -> {
                        DSL.table(DSL.name(request.getName().value)).`as`(DSL.name(tableAlias))
                    }
                }
            ).apply {
                // The ORDER BY clause may require JOINs to other tables
                // e.g. if the ORDER BY clause is on a field in a related table
                addJoinsRequiredForOrderByFields(this, request, relationGraph, tableAlias)
            }.where(
                listOfNotNull(
                    // Add the base WHERE conditions
                    request.query.where?.let { where ->
                        expressionToCondition(
                            e = where,
                            currentTableName = FullyQualifiedTableName(tableAlias),
                            relationGraph = relationGraphWithTableAlias,
                        )
                    },
                    // Add the WHERE-clause JOIN conditions required to connect this table to the previous table
                    previousRelationshipEdge?.let { relEdge ->
                        DSL.and(
                            relEdge.entry.column_mapping.map { (sourceColumn, targetColumn) ->
                                DSL.field(
                                    DSL.name(tableAlias, targetColumn.value),
                                ).eq(
                                    DSL.field(
                                        DSL.name(previousTableAlias, sourceColumn.value),
                                    )
                                )
                            }
                        )
                    }
                )
            ).orderBy(
                translateIROrderByField(
                    orderBy = request.query.order_by,
                    currentTableName = FullyQualifiedTableName(tableAlias),
                    relationGraph = relationGraphWithTableAlias,
                )
            ).limit(
                DSL.inline(request.query.limit ?: MAX_QUERY_ROWS)
            ).offset(
                DSL.inline(request.query.offset ?: 0)
            ).asTable("json")
        )
    }

    protected open fun mkJsonObj(
        request: QueryRequest,
        tableAlias: String,
        relationGraph: OperationRequestRelationGraph
    ): Field<*> {
       return DSL.jsonObject(
           // Select a DSL.jsonEntry() for each non-relationship field in the query
           getQueryColumnFields(request.query.fields ?: emptyMap()).map { (name, field) ->
               DSL.jsonEntry(
                   name.value,
                   DSL.field(
                       DSL.name(tableAlias, field.column),
                       columnFieldTypeTojOOQSQLDataType(field)
                   )
               )
           } +
           // Select a DSL.jsonEntry() for each relationship field in the query
           // Where the entry is ('rel_name', (queryRequestToSQL(relationQueryRequest)))
           getQueryRelationFields(request.query.fields ?: emptyMap()).map { (name, field) ->
               val relEdge = relationGraph.getRelation(
                   currentTable = request.getName(),
                   relationName = field.relationship
               )
               DSL.jsonEntry(
                   name.value,
                   internalQueryRequestToSQL(
                       previousRelationshipEdge = relEdge,
                       previousTableAlias = tableAlias,
                       request = request.copy(
                           target = relEdge.entry.target,
                           query = field.query
                       )
                   )
               )
           }
       )
    }

}
object JsonQueryGenerator : BaseJsonQueryGenerator()
