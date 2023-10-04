package gdc.sqlgen.custom

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import gdc.ir.*
import gdc.ir.Query
import gdc.ir.Target
import gdc.sqlgen.generic.BaseMutationTranslator
import gdc.sqlgen.generic.MutationPermissionCheckFailureException
import gdc.sqlgen.utils.OperationRequestRelationGraph
import org.jooq.*
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.*

object OracleMutationTranslator : BaseMutationTranslator() {

    override fun autoIncPKInsertMutation(
        mutationRequest: MutationRequest,
        op: MutationOperation.InsertMutationOperation,
        ctx: DSLContext,
        returnHandler: (QueryRequest) -> Select<*>
    ): MutationOperationResult {
        val insertSchema = getInsertSchema(mutationRequest, op)
        val primaryKeyColumnNames = getPrimaryKeyCols(insertSchema)

        val rowsHaveDifferentColumns = op.rows.any { row ->
            op.rows.any { otherRow ->
                row.value.keys != otherRow.value.keys
            }
        }
        if (rowsHaveDifferentColumns) return oracleInsertMixedColumnRowsOneByOne(
            mutationRequest,
            ctx,
            primaryKeyColumnNames,
            op
        )

        return super.autoIncPKInsertMutation(
            mutationRequest,
            op,
            ctx,
            returnHandler,
            insertSchema,
            primaryKeyColumnNames
        )
    }

    private fun oracleInsertMixedColumnRowsOneByOne(
        mutationRequest: MutationRequest,
        ctx: DSLContext,
        primaryKeyColumnNames: List<ColumnName>,
        op: MutationOperation.InsertMutationOperation,
    ): MutationOperationResult {
        val insertSchema = mutationRequest.insert_schema.find { schema -> schema.table == op.table }
        requireNotNull(insertSchema) { "No insert schema found for table ${op.table}" }

        val columnInsertFieldsMap = getColumnInsertFieldsMap(insertSchema)

        val insertedIds = op.rows.map { row ->
            val insert = DSL.insertInto(DSL.table(DSL.name(op.table.value)))
                .columns(
                    row.value.keys.map { colName ->
                        // Find the column type from the insert schema
                        val columnEntry = columnInsertFieldsMap[colName]!!
                        DSL.field(
                            DSL.name(columnEntry.column),
                            scalarTypeTojOOQSQLDataType(columnEntry.column_type)
                        )
                    }
                )
                .values(row.value.values)
                .returning(primaryKeyColumnNames.map { DSL.field(it.value, SQLDataType.INTEGER.identity(true)) })


            ctx.transactionResult { txn ->
                val insertedRow = txn.dsl().fetch(insert)
                val insertedId = insertedRow.getValue(0, 0).toString().toLong()
                val postInsertCheckRows = txn.dsl()
                    .selectCount()
                    .from(DSL.table(DSL.name(op.table.value)))
                    .where(
                        DSL.and(
                            primaryKeyColumnNames.map { pk ->
                                DSL.field(pk.value).eq(insertedId)
                            }
                        )
                    )
                    .and(
                        DSL.not(
                            op.post_insert_check?.let { postInsertCheck ->
                                expressionToCondition(
                                    e = postInsertCheck,
                                    currentTableName = op.table,
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

                insertedId
            }
        }


        val returningQuery = OracleGenerator.mutationQueryRequestToSQL(
            QueryRequest(
                target = Target.TableTarget(name = op.table),
                relationships = mutationRequest.relationships,
                query = Query(
                    fields = op.returning_fields?.entries?.associate {
                        Alias(it.key) to it.value
                    },
                    where = Expression.And(
                        mkPrimaryKeyBetweenIR(primaryKeyColumnNames, insertedIds.first(), insertedIds.last())
                    )
                )
            )
        )

        val returningRows =
            ctx.dsl().fetch(returningQuery)
                .getValue(0, 0)
                .toString()
                .let { jacksonObjectMapper().readValue<List<Map<String, Any>>>(it) }

        return MutationOperationResult(
            returning = if (op.returning_fields == null) null else returningRows,
            affected_rows = returningRows.size
        )
    }

    override fun <T> withTempTableTxn(
        ctx: DSLContext,
        tempTableSelectAs: Select<*>,
        txnBlock: (DSLContext, org.jooq.Name) -> T,
    ): T {
        val (tempTableName, createTempTable) = createTemporaryTable(tempTableSelectAs)
        createTempTable.onCommitPreserveRows()

        return ctx.transactionResult { txn ->
            txn.dsl().execute(createTempTable)
            val res = txnBlock(txn.dsl(), DSL.name(tempTableName))
            txn.dsl().execute(DSL.truncateTable(tempTableName))
            txn.dsl().execute(DSL.dropTemporaryTableIfExists(tempTableName))
            res
        }
    }
}
