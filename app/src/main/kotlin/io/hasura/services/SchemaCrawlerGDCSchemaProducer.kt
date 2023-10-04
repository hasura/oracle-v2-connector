package io.hasura.services

import gdc.ir.Capabilities
import gdc.ir.ColumnName
import gdc.ir.ColumnValueGenerationStrategy
import gdc.ir.ConstraintName
import gdc.ir.DetailLevel
import gdc.ir.FullyQualifiedTableName
import gdc.ir.ScalarType
import gdc.ir.Schema
import gdc.ir.SourceColumnName
import gdc.ir.TableType
import gdc.ir.TargetColumnName
import io.hasura.utils.javaSqlTypeToGDCScalar
import org.jboss.logging.Logger
import schemacrawler.schema.Catalog
import schemacrawler.schema.ColumnDataType

/**
 * Converts a [schemacrawler.schema.Catalog] to a [gdc.ir.Schema]
 *
 * @param config allows a user to specify manual primary keys, if the underlying database doesn't support them
 */
class SchemaCrawlerGDCSchemaProducer(private val config: Map<String, Any?>, private val capabilities: Capabilities, private val detailLevel: DetailLevel) {

    private val logger: Logger = Logger.getLogger(SchemaCrawlerGDCSchemaProducer::class.java.name)

    fun makeGDCSchemaForCatalog(catalog: Catalog): Schema {
        return Schema(
            tables = catalog.tables.map(::makeGDCTable)
        )
    }

    private fun makeGDCTable(table: schemacrawler.schema.Table): gdc.ir.Table {
        val fullyQualifyAllNames = when (config["fully_qualify_all_names"]) {
            false -> false
            else -> true
        }

        if (detailLevel == DetailLevel.BASIC_INFO) {
            return gdc.ir.Table(
                name = processTableName(fullyQualifyAllNames, table.fullName),
                type = if (table.type.isView) TableType.VIEW else TableType.TABLE
            )
        } else {
            return gdc.ir.Table(
                name = processTableName(fullyQualifyAllNames, table.fullName),
                type = if (table.type.isView) TableType.VIEW else TableType.TABLE,
                description = table.remarks,
                columns = table.columns.map(::makeGDCColumn),
                primary_key = table.primaryKey
                    ?.let { pk -> pk.constrainedColumns.map { ColumnName(it.name) } }
                    ?: emptyList(),
                foreign_keys = table.importedForeignKeys.associate { fk ->
                    ConstraintName(fk.name) to gdc.ir.ForeignKeyConstraint(
                        foreign_table = processTableName(fullyQualifyAllNames, fk.referencedTable.fullName),
                        column_mapping = fk.constrainedColumns.associate {
                            SourceColumnName(it.name) to TargetColumnName(it.referencedColumn.name)
                        }
                    )
                },
                insertable = capabilities.mutations?.insert != null && !table.type.isView,
                updatable = capabilities.mutations?.update != null && !table.type.isView,
                deletable = capabilities.mutations?.delete != null && !table.type.isView
            )
        }
    }

    private fun makeGDCColumn(column: schemacrawler.schema.Column): gdc.ir.Column {
        val insertable = capabilities.mutations?.insert != null && column.isGenerated.not() && !column.parent.type.isView
        val updatable = capabilities.mutations?.update != null && column.isGenerated.not() && !column.parent.type.isView
        val valueGenerated =
            if (column.isAutoIncremented) {
                ColumnValueGenerationStrategy.AutoIncrement
            } else if (column.hasDefaultValue() && !column.defaultValue.equals("NULL")) {
                ColumnValueGenerationStrategy.DefaultValue
            } else {
                null
            }

        return gdc.ir.Column(
            name = column.name,
            description = column.remarks,
            nullable = column.isNullable,
            insertable = insertable,
            updatable = updatable,
            value_generated = valueGenerated,
            type = javaSqlTypeToGDCScalar(column.type.javaSqlType.vendorTypeNumber)
        )
    }

    // TODO: Supportiung this requires supporting literal names of all database types, which is a pain.
    private fun javaSqlTypeNameToGDCScalar(sqlType: ColumnDataType): ScalarType {
        // For some reason, "JSON" comes through as "Chinook.JSON" for example
        val typeName = sqlType.name.split(".").last().uppercase()
        return when (typeName) {
            "BIT" -> ScalarType.BOOLEAN
            "BOOLEAN" -> ScalarType.BOOLEAN

            "TINYINT" -> ScalarType.INT
            "SMALLINT" -> ScalarType.INT
            "INTEGER" -> ScalarType.INT
            "BIGINT" -> ScalarType.INT

            "FLOAT" -> ScalarType.FLOAT
            "REAL" -> ScalarType.FLOAT
            "DOUBLE" -> ScalarType.FLOAT
            "NUMERIC" -> ScalarType.FLOAT
            "DECIMAL" -> ScalarType.FLOAT

            "CHAR" -> ScalarType.STRING
            "VARCHAR" -> ScalarType.STRING
            "LONGVARCHAR" -> ScalarType.STRING
            "NCHAR" -> ScalarType.STRING
            "NVARCHAR" -> ScalarType.STRING
            "LONGNVARCHAR" -> ScalarType.STRING

            "DATE" -> ScalarType.DATE
            "TIME" -> ScalarType.TIME
            "TIME_WITH_TIMEZONE" -> ScalarType.TIME_WITH_TIMEZONE
            "TIMESTAMP" -> ScalarType.DATETIME
            "TIMESTAMP_WITH_TIMEZONE" -> ScalarType.DATETIME_WITH_TIMEZONE
            // "YEAR" -> ScalarType.YEAR

            // "JSON" -> ScalarType.JSON
            // "JSONB" -> ScalarType.JSON

            "OTHER" -> ScalarType.STRING
            else -> ScalarType.STRING
        }
    }

    companion object {
        // TODO: How resilient is this?
        fun processTableName(fullyQualifyAllNames: Boolean, tableName: String): FullyQualifiedTableName {
            val names = tableName.split(".").map { it.removeSurrounding("\"") }
            return if (fullyQualifyAllNames) {
                FullyQualifiedTableName(names)
            } else {
                FullyQualifiedTableName(names.last())
            }
        }
    }
}
