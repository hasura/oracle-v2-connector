package gdc.sqlgen.generic

import gdc.ir.*
import gdc.sqlgen.utils.IOperationRequestRelationGraph
import gdc.sqlgen.utils.RelationshipEdge
import org.jooq.Condition
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

sealed interface BaseGenerator {

    fun mkSQLJoin(
        rel: RelationshipEdge,
        sourceTableNameTransform: (FullyQualifiedTableName) -> List<String> = { it.value },
        targetTableNameTransform: (FullyQualifiedTableName) -> List<String> = { it.value },
        sourceColNameTransform: (SourceColumnName) -> String = { it.value },
        targetColNameTransform: (TargetColumnName) -> String = { it.value }
    ): Condition {
        return DSL.and(
            rel.entry.column_mapping.map { (sourceColumn, targetColumn) ->
                val sourceTableFQN = sourceTableNameTransform(rel.table)
                val targetTableFQN = targetTableNameTransform(rel.entry.target.getTargetName())
                val sourceColFQN = sourceColNameTransform(sourceColumn)
                val targetColFQN = targetColNameTransform(targetColumn)
                DSL.field(DSL.name(sourceTableFQN + sourceColFQN))
                    .eq(DSL.field(DSL.name(targetTableFQN + targetColFQN)))
            }
        )
    }

    private fun mkRelExistsSubquery(relation: RelationshipEdge, acc: Condition): Condition {
        return DSL.exists(
            DSL.selectOne()
                .from(DSL.table(DSL.name(relation.entry.target.getTargetName().value)))
                .where(mkSQLJoin(relation).and(acc))
        )
    }

    // Convert a WHERE-like expression IR into a JOOQ Condition
    // Used for both "where" expressions and things like "post-insert check" expressions
    // Requires 3 things:
    // 1. The current table alias
    // 2. The relation graph for the request
    // 3. The actual Expression IR object to convert
    fun expressionToCondition(
        e: Expression,
        currentTableName: FullyQualifiedTableName,
        relationGraph: IOperationRequestRelationGraph
    ): Condition {

        fun mkCondition(
            e: HasColumnReference,
            baseCond: Condition,
            relGraph: IOperationRequestRelationGraph,
        ): Condition = when (e.column.path.size) {
            0 -> baseCond
            else -> relGraph
                .traverseRelEdges(e.column.path, currentTableName)
                .foldRight(baseCond, ::mkRelExistsSubquery)
        }


        return when (e) {
            // The negation of a single subexpression
            is Expression.Not -> DSL.not(expressionToCondition(e.expression, currentTableName, relationGraph))

            // A conjunction of several subexpressions
            is Expression.And -> when (e.expressions.size) {
                0 -> DSL.trueCondition()
                else -> DSL.and(e.expressions.map { expressionToCondition(it, currentTableName, relationGraph) })
            }

            // A disjunction of several subexpressions
            is Expression.Or -> when (e.expressions.size) {
                0 -> DSL.falseCondition()
                else -> DSL.or(e.expressions.map { expressionToCondition(it, currentTableName, relationGraph) })
            }

            // Test the specified column against a single value using a particular binary comparison operator
            is Expression.ApplyBinaryComparison -> {
                val baseCond = run {
                    val column = DSL.field(DSL.name(currentTableName.value + e.column.name))
                    val comparisonValue = when (val v = e.value) {
                        is ComparisonValue.ColumnValue -> DSL.field(DSL.name(currentTableName.value + v.column.name))
                        is ComparisonValue.ScalarValue -> when (v.value_type.uppercase()) {
                            ScalarType.STRING.name -> DSL.inline(v.value, SQLDataType.VARCHAR)
                            ScalarType.INT.name -> DSL.inline(v.value, SQLDataType.INTEGER)
                            ScalarType.FLOAT.name -> DSL.inline(v.value, SQLDataType.NUMERIC)
                            ScalarType.NUMBER.name -> DSL.inline(v.value, SQLDataType.NUMERIC)
                            ScalarType.BOOLEAN.name -> DSL.inline(v.value, SQLDataType.BOOLEAN)
                            ScalarType.DATETIME.name -> DSL.inline(v.value)
                            ScalarType.DATE.name -> DSL.inline(v.value)
                            ScalarType.TIME.name -> DSL.inline(v.value)
                            ScalarType.TIME_WITH_TIMEZONE.name -> DSL.inline(v.value)
                            ScalarType.DATETIME_WITH_TIMEZONE.name -> DSL.inline(v.value)

                            else -> DSL.inline(v.value)
                        }
                    }
                    when (e.operator) {
                        ApplyBinaryComparisonOperator.EQUAL -> column.eq(comparisonValue)
                        ApplyBinaryComparisonOperator.LESS_THAN -> column.lt(comparisonValue)
                        ApplyBinaryComparisonOperator.LESS_THAN_OR_EQUAL -> column.le(comparisonValue)
                        ApplyBinaryComparisonOperator.GREATER_THAN -> column.gt(comparisonValue)
                        ApplyBinaryComparisonOperator.GREATER_THAN_OR_EQUAL -> column.ge(comparisonValue)
                        ApplyBinaryComparisonOperator.CONTAINS -> column.contains(comparisonValue)
                    }
                }
                mkCondition(e, baseCond, relationGraph)
            }

            // Test the specified column against a particular unary comparison operator
            is Expression.ApplyUnaryComparison -> {
                val baseCond = run {
                    val column = DSL.field(DSL.name(currentTableName.value + e.column.name))
                    when (e.operator) {
                        ApplyUnaryComparisonOperator.IS_NULL -> column.isNull
                    }
                }
                mkCondition(e, baseCond, relationGraph)
            }

            // Test the specified column against an array of values using a particular binary comparison operator
            is Expression.ApplyBinaryArrayComparison -> {
                val baseCond = run {
                    val column = DSL.field(DSL.name(currentTableName.value + e.column.name))
                    when (e.operator) {
                        ApplyBinaryArrayComparisonOperator.IN -> {
                            when {
                                // Generate "IN (SELECT NULL WHERE 1 = 0)" for easier debugging
                                e.values.isEmpty() -> column.`in`(
                                    DSL.select(DSL.nullCondition())
                                        .where(DSL.inline(1).eq(DSL.inline(0)))
                                )

                                else -> column.`in`(DSL.list(e.values.map { DSL.inline(it) }))
                            }
                        }
                    }
                }
                mkCondition(e, baseCond, relationGraph)
            }

            // Test if a row exists that matches the where subexpression in the specified table (in_table)
            //
            // where (
            //   exists (
            //     select 1 "one"
            //     from "AwsDataCatalog"."chinook"."album"
            //     where (
            //         "AwsDataCatalog"."chinook"."album"."artistid" = "artist_base_fields_0"."artistid"
            //         and "AwsDataCatalog"."chinook"."album"."title" = 'For Those About To Rock We Salute You'
            //         and exists (
            //           select 1 "one"
            //           from "AwsDataCatalog"."chinook"."track"
            //           where (
            //               "AwsDataCatalog"."chinook"."track"."albumid" = "albumid"
            //               and "AwsDataCatalog"."chinook"."track"."name" = 'For Those About To Rock (We Salute You)'
            //           )
            //         )
            //     )
            //   )
            // )
            is Expression.Exists -> {
                when (val inTable = e.in_table) {
                    // The table is related to the current table via the relationship name specified in relationship
                    // (this means it should be joined to the current table via the relationship)
                    is ExistsInTable.RelatedTable -> {
                        val relEdge =
                            relationGraph.getRelation(currentTableName, inTable.relationship)
                        DSL.exists(
                            DSL
                                .selectOne()
                                .from(
                                    DSL.table(DSL.name(relEdge.entry.target.getTargetName().value))
                                )
                                .where(
                                    DSL.and(
                                        relEdge.entry.column_mapping.map { (sourceCol, targetCol) ->
                                            DSL.field(DSL.name(currentTableName.value + sourceCol.value))
                                                .eq(DSL.field(DSL.name(relEdge.entry.target.getTargetName().value + targetCol.value)))
                                        } + expressionToCondition(
                                            e.where,
                                            currentTableName = relEdge.entry.target.getTargetName(),
                                            relationGraph = relationGraph
                                        )
                                    )
                                )
                        )
                    }

                    // The table specified by table is unrelated to the current table and therefore is not explicitly joined to the current table
                    // (this means it should be joined to the current table via a subquery)
                    is ExistsInTable.UnrelatedTable -> {
                        DSL.exists(
                            DSL
                                .selectOne()
                                .from(
                                    DSL.table(DSL.name(inTable.table.value))
                                )
                                .where(
                                    expressionToCondition(
                                        e.where,
                                        currentTableName = inTable.table,
                                        relationGraph = relationGraph
                                    )
                                )
                        )
                    }
                }
            }
        }
    }
}
