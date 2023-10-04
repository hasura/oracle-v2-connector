package io.hasura.services.schemaGenerators

import gdc.ir.ColumnName
import gdc.ir.ConstraintName
import gdc.ir.ForeignKeyConstraint
import gdc.ir.FullyQualifiedTableName
import gdc.ir.TableType

data class TableSchemaRow(
    val tableName: FullyQualifiedTableName,
    val tableType: TableType,
    val description: String?,
    val columns: List<ColumnSchemaRow>?,
    val pks: List<ColumnName>?,
    val fks: Map<ConstraintName, ForeignKeyConstraint>?
)

data class ColumnSchemaRow(
    val name: String,
    val description: String?,
    val type: String,
    val numeric_scale: Int?,
    val nullable: Boolean,
    val auto_increment: Boolean,
    val is_primarykey: Boolean?
)

data class TableFilterJson(
    val s: String,
    val t: String
)

data class FunctionFilterJson(
    val s: String,
    val f: String
)
