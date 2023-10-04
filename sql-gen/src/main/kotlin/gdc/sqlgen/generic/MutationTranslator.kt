package gdc.sqlgen.generic

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import gdc.ir.*
import gdc.ir.Query
import gdc.ir.Target
import gdc.sqlgen.utils.OperationRequestRelationGraph
import org.jooq.*
import org.jooq.Field
import org.jooq.Table
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID

class MutationPermissionCheckFailureException(message: String) : Exception(message)

abstract class BaseMutationTranslator : BaseGenerator {

    fun translate(
        mutationRequest: MutationRequest,
        ctx: DSLContext,
        returnHandler: (QueryRequest) -> Select<*>
    ): List<MutationOperationResult> {
        return mutationRequest.operations.map { op ->
            when (op) {
                is MutationOperation.InsertMutationOperation -> {
                    val insertSchema = mutationRequest.insert_schema.find { schema -> schema.table == op.table }
                    requireNotNull(insertSchema) { "No insert schema found for table ${op.table}" }

                    // If there's no AUTO_INCREMENT PK, we need to create a temp table and insert into it
                    val hasAutoIncPK =
                        getColumnInsertSchemas(insertSchema).any { columnInsertSchema -> columnInsertSchema.value_generated is ColumnValueGenerationStrategy.AutoIncrement }

                    if (!hasAutoIncPK) {
                        println("Table ${op.table} has no auto-increment PK, using temp table")
                        nonAutoIncPKTempTableInsertMutation(mutationRequest, op, ctx, returnHandler)
                    } else {
                        println("Table ${op.table} has auto-increment PK")
                        autoIncPKInsertMutation(mutationRequest, op, ctx, returnHandler)
                    }
                }

                is MutationOperation.UpdateMutationOperation -> {
                    updateMutation(mutationRequest, op, ctx, returnHandler)
                }

                is MutationOperation.DeleteMutationOperation -> {
                    deleteMutation(mutationRequest, op, ctx, returnHandler)
                }
            }
        }
    }

    private fun getColumnInsertSchemas(tableInsertSchema: TableInsertSchema): List<InsertFieldSchema.ColumnInsertSchema> {
        return tableInsertSchema.fields.values.filterIsInstance(InsertFieldSchema.ColumnInsertSchema::class.java)
    }

    // For tables that don't have auto-increment PK, we need to create a temp table and insert into it
    // before we can insert into the target table.
    //
    // This is so that we can emulate RETURNING and have access to the inserted rows.
    // This method will be called in the case when the table:
    // - Has no PK
    // - Has a PK that is not auto-increment
    private fun nonAutoIncPKTempTableInsertMutation(
        mutation: MutationRequest,
        op: MutationOperation.InsertMutationOperation,
        ctx: DSLContext,
        returnHandler: (QueryRequest) -> Select<*>
    ): MutationOperationResult {
        // We need to extract the union of all column names in the rows
        // This is so we can set default values for columns that aren't given in some rows
        // to have the same column count across all rows, or else jOOQ will throw an error
        val insertSchema = mutation.insert_schema.find { schema -> schema.table == op.table }
        requireNotNull(insertSchema) { "No insert schema found for table ${op.table}" }

        val (columns, rows) = getInsertColumnsAndRows(insertSchema, op.rows)


        // WARNING: DO NOT USE "ctx" INSIDE THE BLOCK, USE "txn" INSTEAD
        return ctx.transactionResult { txn ->

            // CREATE TEMPORARY TABLE temp_table AS SELECT * FROM target_table LIMIT 0
            val tempTableDefinition = DSL.select().from(DSL.name(op.table.value)).limit(0)
            val (tempTableName, createTemporaryTable) = createTemporaryTable(tempTableDefinition)

            // INSERT INTO temp_table (columns) VALUES (rows)
            val insertIntoTempTable = DSL.insertInto(DSL.table(DSL.name(tempTableName)))
                .columns(columns)
                .valuesOfRows(rows)

            // INSERT INTO target_table SELECT * FROM temp_table
            val insertIntoTargetFromTemp = DSL.insertInto(DSL.table(DSL.name(op.table.value)))
                .select(DSL.select().from(DSL.table(DSL.name(tempTableName))))

            txn.dsl().batch(
                createTemporaryTable,
                insertIntoTempTable,
                insertIntoTargetFromTemp
            ).execute()

            // SELECT COUNT(*) FROM temp_table WHERE NOT (post_insert_check ?: true)
            val postInsertCheckRows = txn.dsl()
                .selectCount()
                .from(DSL.name(tempTableName))
                .where(
                    DSL.not(
                        op.post_insert_check?.let { postInsertCheck ->
                            expressionToCondition(
                                e = postInsertCheck,
                                currentTableName = FullyQualifiedTableName(tempTableName),
                                relationGraph = OperationRequestRelationGraph(mutation)
                            )
                        } ?: DSL.trueCondition()
                    )
                )
                .fetchOne(0, Int::class.java)

            postInsertCheckRows?.let {
                if (it > 0) {
                    // Rollback transaction
                    throw MutationPermissionCheckFailureException("Post-insert check failed for $it rows")
                }
            }

            val returningQuery = returnHandler(
                QueryRequest(
                    target = Target.TableTarget(
                        name = FullyQualifiedTableName(tempTableName)
                    ),
                    // Need to replace the source table with the temp table name for the OperationRequestRelationGraph
                    relationships = mutation.relationships.map{ it as TableRelationship }
                        .map {
                            if (it.source_table == op.table) {
                                it.copy(source_table = FullyQualifiedTableName(tempTableName))
                            } else {
                                it
                            }
                        },
                    query = Query(
                        fields = op.returning_fields?.entries?.associate {
                            Alias(it.key) to it.value
                        }
                    )
                )
            )

            val returningRows = txn.dsl().fetch(returningQuery)
                .getValue(0, 0)
                .toString()
                .let { jacksonObjectMapper().readValue<List<Map<String, Any>>>(it) }

            MutationOperationResult(
                returning = if (op.returning_fields == null) null else returningRows,
                affected_rows = returningRows.size
            )

        }
    }

    protected open fun createTemporaryTable(tempTableDefinition: Select<*>): Pair<String, CreateTableWithDataStep> {
        val uuid = UUID.randomUUID().toString().replace("-", "_")
        val tempTableName = "temp_table_$uuid"
        return tempTableName to DSL.createGlobalTemporaryTable(tempTableName).`as`(tempTableDefinition)
    }


    protected fun getInsertSchema(mutationRequest: MutationRequest, op: MutationOperation.InsertMutationOperation): TableInsertSchema {
        // We need to extract the union of all column names in the rows
        // This is so we can set default values for columns that aren't given in some rows
        // to have the same column count across all rows, or else jOOQ will throw an error
        val insertSchema = mutationRequest.insert_schema.find { schema -> schema.table == op.table }
        requireNotNull(insertSchema) { "No insert schema found for table ${op.table}" }
        return insertSchema
    }

    protected fun getPrimaryKeyCols(insertSchema: TableInsertSchema): List<ColumnName> {
        // Get the primary key columns, we'll use this later to fetch the rows in the post-insert check and RETURNING
        val primaryKeyColumnNames = insertSchema.primary_key ?: emptyList()
        require(primaryKeyColumnNames.isNotEmpty()) { "No primary key found for table with auto-inc columns: ${insertSchema.table}" }
        return primaryKeyColumnNames
    }

    protected open fun autoIncPKInsertMutation(
        mutationRequest: MutationRequest,
        op: MutationOperation.InsertMutationOperation,
        ctx: DSLContext,
        returnHandler: (QueryRequest) -> Select<*>): MutationOperationResult {
        val insertSchema = getInsertSchema(mutationRequest,op)
        val primaryKeyColumnNames = getPrimaryKeyCols(insertSchema)
        return autoIncPKInsertMutation(
            mutationRequest,
            op,
            ctx,
            returnHandler,
            insertSchema,
            primaryKeyColumnNames
        )
    }

    protected fun autoIncPKInsertMutation(
        mutationRequest: MutationRequest,
        op: MutationOperation.InsertMutationOperation,
        ctx: DSLContext,
        returnHandler: (QueryRequest) -> Select<*>,
        insertSchema: TableInsertSchema,
        primaryKeyColumnNames: List<ColumnName>
    ): MutationOperationResult {
        return ctx.transactionResult { txn ->
            // BEGIN IMPLICIT TRANSACTION
            val (columns, rows) = getInsertColumnsAndRows(insertSchema, op.rows)

            // If a row doesn't have a value for a column, we need to insert a default value
            val insertQuery = DSL.insertInto(DSL.table(DSL.name(op.table.value)))
                .columns(columns)
                .valuesOfRows(rows)
                .returning(primaryKeyColumnNames.map { DSL.field(it.value, SQLDataType.INTEGER.identity(true)) })

            val affectedRows = txn.dsl().fetch(insertQuery)

            val firstInsertedId = affectedRows.getValue(0, 0).toString().toLong()
            val lastInsertedId = affectedRows.getValue(affectedRows.size - 1, 0).toString().toLong()

            // Check the number of rows which fail the post-insert check
            val postInsertCheckRows = txn.dsl()
                .selectCount()
                .from(DSL.name(op.table.value))
                .where(
                    DSL.not(
                        op.post_insert_check?.let { postInsertCheck ->
                            expressionToCondition(
                                e = postInsertCheck,
                                currentTableName = op.table,
                                relationGraph = OperationRequestRelationGraph(mutationRequest)
                            )
                        } ?: DSL.trueCondition()
                    )
                ).and(
                    DSL.and(
                        primaryKeyColumnNames.map { col ->
                            DSL.field(DSL.name(col.value))
                                .between(firstInsertedId, lastInsertedId)
                        }
                    )
                )
                .fetchOne(0, Int::class.java)

            postInsertCheckRows?.let {
                if (it > 0) {
                    // Rollback transaction
                    throw MutationPermissionCheckFailureException("Post-insert check failed for $it rows")
                }
            }

            val returningQuery = returnHandler(
                QueryRequest(
                    target = Target.TableTarget(
                        name = op.table
                    ),
                    relationships = mutationRequest.relationships,
                    query = Query(
                        fields = op.returning_fields?.entries?.associate {
                            Alias(it.key) to it.value
                        },
                        where = Expression.And(
                            mkPrimaryKeyBetweenIR(primaryKeyColumnNames, firstInsertedId, lastInsertedId)
                        ),
                    )
                )
            )

            val returningRows = txn.dsl().fetch(returningQuery)
                .getValue(0, 0)
                .toString()
                .let { jacksonObjectMapper().readValue<List<Map<String, Any>>>(it) }

            MutationOperationResult(
                affected_rows = affectedRows.size,
                returning = if (op.returning_fields == null) null else returningRows
            )
            // END IMPLICIT TRANSACTION
        }
    }

    private fun getInsertColumnsAndRows(
        insertSchema: TableInsertSchema,
        rowsObjs: List<RowObject>
    ): Pair<List<Field<*>>, List<RowN>> {
        val allUsedFields = rowsObjs.asSequence().flatMap { it.value.keys }.toSet()
        val columnInsertFieldsMap = getColumnInsertFieldsMap(insertSchema)
        val columns = allUsedFields.map {
            val columnInsertSchema = columnInsertFieldsMap[it]
            requireNotNull(columnInsertSchema) { "No insert schema found for field $it in table ${insertSchema.table}" }
            DSL.field(
                // The SQLDataType here are required by Oracle for RETURNING support
                columnInsertSchema.column,
                scalarTypeTojOOQSQLDataType(columnInsertSchema.column_type)
            )
        }

        val rows = rowsObjs.map { row ->
            // Create a DSL.row for each row, with the values in the same order as the columns
            // This is so we can use the DSL.row constructor that takes a list of values
            DSL.row(allUsedFields.map { rowField ->
                row.value[rowField] ?: DSL.defaultValue()
            })
        }

        return Pair(columns, rows)
    }

    protected fun getColumnInsertFieldsMap(insertSchema: TableInsertSchema) = insertSchema.fields
        .filter { it.value is InsertFieldSchema.ColumnInsertSchema }
        .map { (k, v) -> k to v as InsertFieldSchema.ColumnInsertSchema }
        .toMap()

    protected fun mkPrimaryKeyBetweenIR(
        primaryKeyColumnNames: List<ColumnName>,
        firstInsertedId: Long,
        lastInsertedId: Long
    ) = primaryKeyColumnNames.map { pkColName ->
        Expression.And(
            listOf(
                Expression.ApplyBinaryComparison(
                    operator = ApplyBinaryComparisonOperator.GREATER_THAN_OR_EQUAL,
                    value = ComparisonValue.ScalarValue(
                        value = firstInsertedId.toString(),
                        value_type = ScalarType.INT.name
                    ),
                    column = ColumnReference(
                        path = emptyList(),
                        name = pkColName.value
                    )
                ),
                Expression.ApplyBinaryComparison(
                    operator = ApplyBinaryComparisonOperator.LESS_THAN_OR_EQUAL,
                    value = ComparisonValue.ScalarValue(
                        value = lastInsertedId.toString(),
                        value_type = ScalarType.INT.name
                    ),
                    column = ColumnReference(
                        path = emptyList(),
                        name = pkColName.value
                    )
                )
            )
        )
    }

    // Runs a block of code with a temporary table inside of a transaction
    protected open fun <T> withTempTableTxn(
        ctx: DSLContext,
        tempTableSelectAs: Select<*>,
        txnBlock: (DSLContext, org.jooq.Name) -> T,
    ): T {
        val (tempTableName, createTempTable) = createTemporaryTable(tempTableSelectAs)

        return ctx.transactionResult { txn ->
            txn.dsl().execute(createTempTable)
            val res = txnBlock(txn.dsl(), DSL.name(tempTableName))
            txn.dsl().execute(DSL.dropTemporaryTableIfExists(tempTableName))
            res
        }
    }

    private fun deleteMutation(
        mutationRequest: MutationRequest,
        op: MutationOperation.DeleteMutationOperation,
        ctx: DSLContext,
        returnHandler: (QueryRequest) -> Select<*>
    ): MutationOperationResult {
        // The where condition is used both to select the rows to populate the temp table, and to select the rows to delete
        val whereCond = op.where?.let { e ->
            expressionToCondition(
                e,
                currentTableName = op.table,
                OperationRequestRelationGraph(mutationRequest)
            )
        } ?: DSL.trueCondition()

        val tempTableRows = ctx.selectFrom(DSL.table(DSL.name(op.table.value))).where(whereCond)

        // WARNING: DO NOT USE "ctx" INSIDE THE BLOCK, USE "txn" INSTEAD
        return withTempTableTxn(ctx, tempTableRows) { txn, tempTableName ->
            // Delete the rows from the original table
            val deleteQuery = DSL.deleteFrom(DSL.table(DSL.name(op.table.value))).where(whereCond)

            // Execute the delete query
            val affectedRows = txn.execute(deleteQuery)

            // Now, we need to select the deleted rows from the temp table
            val returningQuery = returnHandler(
                QueryRequest(
                    target = Target.TableTarget(
                        name = FullyQualifiedTableName(*tempTableName.name)
                    ),
                    relationships = mutationRequest.relationships.map{ it as TableRelationship}
                        .map {
                            if (it.source_table == op.table) {
                                it.copy(source_table = FullyQualifiedTableName(*tempTableName.name))
                            } else {
                                it
                            }
                        },
                    query = Query(
                        fields = op.returning_fields?.entries?.associate {
                            Alias(it.key) to it.value
                        }
                    )
                )
            )

            val returningRows = txn.fetch(returningQuery)
                .getValue(0, 0)
                .toString()
                .let { jacksonObjectMapper().readValue<List<Map<String, Any>>>(it) }

            MutationOperationResult(
                affected_rows = affectedRows,
                returning = if (op.returning_fields == null) null else returningRows
            )
        }
    }

    private fun mkUpdateValue(update: RowUpdate): org.jooq.Param<*> {
        return when (update.value_type) {
            ScalarType.INT -> DSL.inline(update.value, SQLDataType.INTEGER)
            ScalarType.FLOAT -> DSL.inline(update.value, SQLDataType.FLOAT)
            ScalarType.NUMBER -> DSL.inline(update.value, SQLDataType.FLOAT)
            ScalarType.STRING -> DSL.inline(update.value, SQLDataType.VARCHAR)
            ScalarType.BOOLEAN -> DSL.inline(update.value, SQLDataType.BOOLEAN)
            ScalarType.DATETIME -> DSL.inline(update.value, SQLDataType.TIMESTAMP)
            ScalarType.DATETIME_WITH_TIMEZONE -> DSL.inline(update.value, SQLDataType.TIMESTAMPWITHTIMEZONE)
            ScalarType.DATE -> DSL.inline(update.value, SQLDataType.DATE)
            ScalarType.TIME -> DSL.inline(update.value, SQLDataType.TIME)
            ScalarType.TIME_WITH_TIMEZONE -> DSL.inline(update.value, SQLDataType.TIMEWITHTIMEZONE)
        }
    }

    protected fun scalarTypeTojOOQSQLDataType(scalarType: ScalarType): DataType<*> {
        return when (scalarType) {
            ScalarType.BOOLEAN -> SQLDataType.BOOLEAN
            ScalarType.DATETIME -> SQLDataType.TIMESTAMP
            ScalarType.FLOAT -> SQLDataType.FLOAT
            ScalarType.NUMBER -> SQLDataType.FLOAT
            ScalarType.INT -> SQLDataType.INTEGER
            ScalarType.STRING -> SQLDataType.VARCHAR
            ScalarType.DATETIME_WITH_TIMEZONE -> SQLDataType.TIMESTAMPWITHTIMEZONE
            ScalarType.DATE -> SQLDataType.DATE
            ScalarType.TIME -> SQLDataType.TIME
            ScalarType.TIME_WITH_TIMEZONE -> SQLDataType.TIMEWITHTIMEZONE
        }
    }

    private fun mkUpdateForTable(
        mutationRequest: MutationRequest,
        op: MutationOperation.UpdateMutationOperation,
        table: org.jooq.Table<*>
    ) =
        DSL.update(table)
            .set(op.updates.associate { update ->
                when (update) {
                    is RowUpdate.IncrementalColumnRowUpdate -> {
                        val field = DSL.field(DSL.name(update.column))
                        field to field.plus(DSL.inline(update.value))
                    }

                    is RowUpdate.SetColumnRowUpdate -> {
                        val field = DSL.field(DSL.name(update.column))
                        val newValue = mkUpdateValue(update)
                        field to newValue
                    }

                    is RowUpdate.CustomOperatorRowUpdate -> {
                        val field = DSL.field(DSL.name(update.column))
                        val newValue = mkUpdateValue(update)
                        val rowOp = when (update.operator_name) {
                            "inc" -> field.plus(newValue)
                            "dec" -> field.minus(newValue)
                            else -> throw IllegalArgumentException("Unsupported operator ${update.operator_name}")
                        }
                        field to rowOp
                    }

                    else -> {
                        throw IllegalArgumentException("Unsupported update type ${update::class}")
                    }
                }
            })
            .where(
                op.where?.let { e ->
                    expressionToCondition(
                        e,
                        currentTableName = FullyQualifiedTableName(*table.qualifiedName.name),
                        OperationRequestRelationGraph(mutationRequest)
                    )
                } ?: DSL.trueCondition()
            )

    private fun updateMutation(
        mutationRequest: MutationRequest,
        op: MutationOperation.UpdateMutationOperation,
        ctx: DSLContext,
        returnHandler: (QueryRequest) -> Select<*>
    ): MutationOperationResult {
        // The rows that the temporary table will be populated with
        val tempTableRows = DSL
            .selectFrom(DSL.table(DSL.name(op.table.value)))
            .where(op.where?.let { e ->
                expressionToCondition(
                    e,
                    currentTableName = op.table,
                    OperationRequestRelationGraph(mutationRequest)
                )
            } ?: DSL.trueCondition())

        // WARNING: DO NOT USE "ctx" INSIDE THE BLOCK, USE "txn" INSTEAD
        return withTempTableTxn(ctx, tempTableRows) { txn, tempTableName ->
            // Update the rows in the temp table
            val updateTempTableQuery = mkUpdateForTable(mutationRequest, op, DSL.table(tempTableName))

            // Execute the update query
            val affectedRows = txn.execute(updateTempTableQuery)

            // Check the number of rows which fail the post-insert check
            val postInsertCheckRows = txn.dsl()
                .selectCount()
                .from(DSL.name(tempTableName))
                .where(
                    DSL.not(
                        op.post_update_check?.let { postUpdateCheck ->
                            expressionToCondition(
                                e = postUpdateCheck,
                                currentTableName = FullyQualifiedTableName(*tempTableName.name),
                                relationGraph = OperationRequestRelationGraph(mutationRequest)
                            )
                        } ?: DSL.trueCondition()
                    )
                )
                .fetchOne(0, Int::class.java)

            postInsertCheckRows?.let {
                if (it > 0) {
                    // Rollback transaction
                    throw MutationPermissionCheckFailureException("Post-insert check failed for $it rows")
                }
            }

            // Update the rows in the original table
            val updateOriginalTableQuery = mkUpdateForTable(mutationRequest, op, DSL.table(DSL.name(op.table.value)))

            // Execute the update query
            txn.execute(updateOriginalTableQuery)

            // Now, we need to select the updated rows from the temp table
            val returningQuery = returnHandler(
                QueryRequest(
                    target = Target.TableTarget(
                        name = FullyQualifiedTableName(*tempTableName.name),
                    ),
                    // Need to replace the source table with the temp table name for the OperationRequestRelationGraph
                    relationships = mutationRequest.relationships.map{ it as TableRelationship }
                        .map{
                            if (it.source_table == op.table) {
                                it.copy(source_table = FullyQualifiedTableName(*tempTableName.name))
                            } else {
                                it
                            }
                        },
                    query = Query(
                        fields = op.returning_fields?.entries?.associate {
                            Alias(it.key) to it.value
                        }
                    )
                )
            )

            val returningRows = txn.fetch(returningQuery)
                .getValue(0, 0)
                .toString()
                .let { jacksonObjectMapper().readValue<List<Map<String, Any>>>(it) }

            MutationOperationResult(
                affected_rows = affectedRows,
                returning = if (op.returning_fields == null) null else returningRows
            )
        }
    }
}

object MutationTranslator : BaseMutationTranslator()
