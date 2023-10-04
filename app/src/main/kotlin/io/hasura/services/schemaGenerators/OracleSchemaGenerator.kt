package io.hasura.services.schemaGenerators

import com.fasterxml.jackson.module.kotlin.readValue
import gdc.ir.Capabilities
import gdc.ir.ColumnName
import gdc.ir.ConstraintName
import gdc.ir.DetailLevel
import gdc.ir.ForeignKeyConstraint
import gdc.ir.FullyQualifiedTableName
import gdc.ir.FunctionInfo
import gdc.ir.ScalarType
import gdc.ir.TableType
import java.sql.Connection

class OracleSchemaGenerator(
    capabilities: Capabilities
) : BaseSchemaGenerator(capabilities) {

    override fun queryDatabase(connection: Connection, database: String?, schemas: List<String>, detailLevel: DetailLevel, onlyTablesFilter: List<FullyQualifiedTableName>?): List<TableSchemaRow> {
        val schemaSelection = "(${schemas.joinToString{_ -> "?"}})"
        val (tablesJoinFilterSql, tablesJoinFilterParamValue) =
            if (onlyTablesFilter != null) {
                val tablesJoinFilterParamValue = objectMapper.writeValueAsString(onlyTablesFilter.map { TableFilterJson(it.value[0], it.value[1]) })
                val tablesJoinFilterSql = """
                    INNER JOIN (
                        SELECT jt.*
                        FROM json_table(?, '$[*]' COLUMNS (schema VARCHAR2 PATH '$.s', tablename VARCHAR2 PATH '$.t')) AS jt
                    ) tables_filter ON tables.OWNER = tables_filter.schema AND tables.TABLE_NAME = tables_filter.tablename
                """

                Pair(tablesJoinFilterSql, listOf(tablesJoinFilterParamValue))
            } else {
                Pair("", emptyList())
            }
        val baseTableSql = """
            SELECT *
            FROM (
                SELECT tables.OWNER, tables.TABLE_NAME, 'TABLE' AS TABLE_TYPE
                FROM ALL_TABLES tables
                WHERE tables.TEMPORARY = 'N'
                UNION ALL
                SELECT views.OWNER, views.VIEW_NAME, 'VIEW' AS TABLE_TYPE
                FROM ALL_VIEWS views
            ) tables
            $tablesJoinFilterSql
            WHERE tables.OWNER IN $schemaSelection
        """

        val sql = if (detailLevel == DetailLevel.BASIC_INFO) {
            """
            SELECT
                json_array(tables.OWNER, tables.TABLE_NAME) AS TableName,
                tables.TABLE_TYPE AS TableType,
                NULL AS Description,
                NULL AS Columns,
                NULL AS PrimaryKeys,
                NULL AS ForeignKeys
            FROM (
                $baseTableSql
            ) tables
            """.trimIndent()
        } else {
            """
            SELECT
                json_array(tables.OWNER, tables.TABLE_NAME) AS TableName,
                tables.TABLE_TYPE AS TableType,
                table_comments.COMMENTS AS Description,
                columns.Columns,
                pks.PrimaryKeys,
                fks.ForeignKeys
            FROM (
                $baseTableSql
            ) tables
            INNER JOIN (
                    SELECT OWNER, TABLE_NAME, COMMENTS FROM ALL_TAB_COMMENTS
                    UNION ALL
                    SELECT OWNER, MVIEW_NAME AS TABLE_NAME, COMMENTS FROM ALL_MVIEW_COMMENTS
                ) table_comments
                ON tables.OWNER = table_comments.OWNER AND tables.TABLE_NAME = table_comments.TABLE_NAME
            INNER JOIN ALL_USERS users
                ON users.USERNAME = tables.OWNER
            LEFT JOIN ( -- Must be a LEFT JOIN because INNER performs very poorly for an unknown reason
                    SELECT
                        columns.OWNER,
                        columns.TABLE_NAME,
                        (
                            json_arrayagg(
                                json_object(
                                    'name' VALUE columns.COLUMN_NAME,
                                    'description' VALUE column_comments.COMMENTS,
                                    'type' VALUE columns.DATA_TYPE,
                                    'numeric_scale' VALUE columns.DATA_SCALE,
                                    'nullable' VALUE case when columns.NULLABLE = 'Y' then 'true' else 'false' end,
                                    'auto_increment' VALUE case when columns.IDENTITY_COLUMN = 'YES' then 'true' else 'false' end
                                )
                                ORDER BY columns.COLUMN_ID
                                RETURNING CLOB
                            )
                        ) AS Columns
                    FROM ALL_TAB_COLUMNS columns
                    LEFT OUTER JOIN ALL_COL_COMMENTS column_comments
                        ON columns.OWNER = column_comments.OWNER
                        AND columns.TABLE_NAME = column_comments.TABLE_NAME
                        AND columns.COLUMN_NAME = column_comments.COLUMN_NAME
                    GROUP BY columns.OWNER, columns.TABLE_NAME
                )
                columns
                ON columns.OWNER = tables.OWNER
                AND columns.TABLE_NAME = tables.TABLE_NAME
            LEFT OUTER JOIN (
                    SELECT
                        pk_constraints.OWNER,
                        pk_constraints.TABLE_NAME,
                        (
                            json_arrayagg(
                                pk_columns.COLUMN_NAME
                                ORDER BY pk_columns.POSITION
                                RETURNING CLOB
                            )
                        ) AS PrimaryKeys
                    FROM ALL_CONSTRAINTS pk_constraints
                    LEFT OUTER JOIN ALL_CONS_COLUMNS pk_columns
                        ON pk_constraints.CONSTRAINT_NAME = pk_columns.CONSTRAINT_NAME
                        AND pk_constraints.OWNER = pk_columns.OWNER
                        AND pk_constraints.TABLE_NAME = pk_columns.TABLE_NAME
                    WHERE pk_constraints.CONSTRAINT_TYPE = 'P'
                    GROUP BY pk_constraints.OWNER, pk_constraints.TABLE_NAME
                )
                pks
                ON pks.OWNER = tables.OWNER
                AND pks.TABLE_NAME = tables.TABLE_NAME
            LEFT OUTER JOIN LATERAL (
                    SELECT
                        fks.OWNER,
                        fks.TABLE_NAME,
                        (
                            json_objectagg (
                                fks.FK_CONSTRAINT_NAME VALUE fks.Constraint
                                RETURNING CLOB
                            )
                        ) AS ForeignKeys
                    FROM (
                        SELECT
                            fk_constraints.OWNER,
                            fk_constraints.TABLE_NAME,
                            fk_constraints.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME,
                            json_object(
                                'foreign_table' VALUE json_array(fk_pk_constraints.OWNER, fk_pk_constraints.TABLE_NAME),
                                'column_mapping' VALUE (
                                    json_objectagg (
                                        fk_columns.COLUMN_NAME VALUE fk_pk_columns.COLUMN_NAME
                                    )
                                )
                            ) AS Constraint
                        FROM ALL_CONSTRAINTS fk_constraints
                        INNER JOIN ALL_CONSTRAINTS fk_pk_constraints
                            ON fk_pk_constraints.OWNER = fk_constraints.R_OWNER
                            AND fk_pk_constraints.CONSTRAINT_NAME = fk_constraints.R_CONSTRAINT_NAME
                        INNER JOIN ALL_CONS_COLUMNS fk_columns
                            ON fk_columns.OWNER = fk_constraints.OWNER
                            AND fk_columns.TABLE_NAME = fk_constraints.TABLE_NAME
                            AND fk_columns.CONSTRAINT_NAME = fk_constraints.CONSTRAINT_NAME
                        INNER JOIN ALL_CONS_COLUMNS fk_pk_columns
                            ON fk_pk_columns.OWNER = fk_pk_constraints.OWNER
                            AND fk_pk_columns.TABLE_NAME = fk_pk_constraints.TABLE_NAME
                            AND fk_pk_columns.CONSTRAINT_NAME = fk_pk_constraints.CONSTRAINT_NAME
                            AND fk_pk_columns.POSITION = fk_columns.POSITION
                            AND fk_constraints.CONSTRAINT_TYPE = 'R'
                        WHERE fk_constraints.OWNER = tables.OWNER AND fk_constraints.TABLE_NAME = tables.TABLE_NAME
                        GROUP BY fk_constraints.OWNER, fk_constraints.TABLE_NAME, fk_constraints.CONSTRAINT_NAME, fk_pk_constraints.OWNER, fk_pk_constraints.TABLE_NAME
                    ) fks
                    GROUP BY fks.OWNER, fks.TABLE_NAME
                )
                fks
                ON fks.OWNER = tables.OWNER
                AND fks.TABLE_NAME = tables.TABLE_NAME
            WHERE users.ORACLE_MAINTAINED = 'N'
            """.trimIndent()
        }

        connection.prepareStatement(sql).use { statement ->

            val tablesJoinFilterParams = tablesJoinFilterParamValue.map { paramVal ->
                { index: Int ->
                    val clob = connection.createClob()
                    clob.setString(1, paramVal)
                    statement.setClob(index, clob)
                }
            }
            val schemaParams = schemas.map { schema -> { index: Int -> statement.setString(index, schema) } }
            (tablesJoinFilterParams + schemaParams).mapIndexed { index, applyParam -> applyParam(index + 1) }

            statement.executeQuery()
            val resultSet = statement.resultSet

            val tables = mutableListOf<TableSchemaRow>()
            while (resultSet.next()) {
                tables.add(
                    TableSchemaRow(
                        objectMapper.readValue<FullyQualifiedTableName>(resultSet.getString("TABLENAME")),
                        when (val tableType = resultSet.getString("TABLETYPE")) {
                            "TABLE" -> TableType.TABLE
                            "VIEW" -> TableType.VIEW
                            else -> throw Exception("Unknown table type: $tableType")
                        },
                        resultSet.getString("DESCRIPTION"),
                        objectMapper.readValue<List<ColumnSchemaRow>?>(resultSet.getString("COLUMNS") ?: "null"),
                        objectMapper.readValue<List<ColumnName>?>(resultSet.getString("PRIMARYKEYS") ?: "null"),
                        objectMapper.readValue<Map<ConstraintName, ForeignKeyConstraint>?>(
                            resultSet.getString("FOREIGNKEYS") ?: "null"
                        )
                    )
                )
            }
            return tables
        }
    }

    override fun mapScalarType(columnTypeStr: String, numericScale: Int?): ScalarType {
        return when (columnTypeStr.uppercase()) {
            "VARCHAR2" -> ScalarType.STRING
            "NVARCHAR2" -> ScalarType.STRING
            "NUMBER" -> if (numericScale != null && numericScale > 0) ScalarType.FLOAT else ScalarType.INT
            "FLOAT" -> ScalarType.FLOAT
            "LONG" -> ScalarType.INT
            "DATE" -> ScalarType.DATE
            "ROWID" -> ScalarType.STRING
            "UROWID" -> ScalarType.STRING
            "CHAR" -> ScalarType.STRING
            "NCHAR" -> ScalarType.STRING
            "CLOB" -> ScalarType.STRING
            "NCLOB" -> ScalarType.STRING
            "JSON" -> ScalarType.STRING
            "BOOLEAN" -> ScalarType.BOOLEAN
            else -> ScalarType.STRING
        }
    }

    override fun queryDatabaseFunctions(
        connection: Connection,
        database: String?,
        schemas: List<String>,
        detailLevel: DetailLevel,
        onlyFunctionsFilter: List<FullyQualifiedTableName>?
    ): List<FunctionInfo> {
        // TODO: We do not support functions for oracle yet.
        return emptyList()
    }
}
