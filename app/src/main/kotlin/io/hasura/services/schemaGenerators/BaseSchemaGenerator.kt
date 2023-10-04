package io.hasura.services.schemaGenerators

import com.fasterxml.jackson.databind.ObjectMapper
import gdc.ir.Capabilities
import gdc.ir.Column
import gdc.ir.ColumnValueGenerationStrategy
import gdc.ir.DetailLevel
import gdc.ir.ForeignKeyConstraint
import gdc.ir.FullyQualifiedTableName
import gdc.ir.FunctionInfo
import gdc.ir.ScalarType
import gdc.ir.Schema
import gdc.ir.SchemaRequest
import gdc.ir.Table
import gdc.ir.TableType
import io.quarkus.arc.Arc
import java.sql.Connection

abstract class BaseSchemaGenerator(
    val capabilities: Capabilities
) {

    val objectMapper = Arc.container().instance(ObjectMapper::class.java).get()

    fun getSchema(connection: Connection, fullyQualifyTableNames: Boolean, schemaSelection: List<String> = emptyList(), schemaRequest: SchemaRequest): Schema {
        val database = connection.catalog
        val schema = connection.schema
        val schemaList = when {
            schemaSelection.size > 0 -> schemaSelection
            schema != null -> listOf(schema)
            else -> emptyList()
        }
        val schemas = schemaList.map { it.trim() }
        val onlyTablesFilter = schemaRequest.filters?.only_tables?.map { fullyQualifyTableName(database, schema, it) }
        val onlyFunctionsFilter = schemaRequest.filters?.only_functions?.map { fullyQualifyTableName(database, schema, it) }
        val tableSchemaRows = queryDatabase(connection, database, schemas, schemaRequest.detail_level, onlyTablesFilter)
        val functionInfos = queryDatabaseFunctions(connection, database, schemas, schemaRequest.detail_level, onlyFunctionsFilter)
        val handleTableNameQualification = getHandleTableNameQualification(database, schema, fullyQualifyTableNames)
        return Schema(
            tables = tableSchemaRows.map { mapTable(it, schemaRequest.detail_level, handleTableNameQualification) },
            functions = functionInfos.map {
                it.copy(name = handleTableNameQualification(it.name))
            }
        )
    }

    abstract fun queryDatabase(connection: Connection, database: String?, schemas: List<String>, detailLevel: DetailLevel, onlyTablesFilter: List<FullyQualifiedTableName>?): List<TableSchemaRow>
    abstract fun queryDatabaseFunctions(connection: Connection, database: String?, schemas: List<String>, detailLevel: DetailLevel, onlyFunctionsFilter: List<FullyQualifiedTableName>?): List<FunctionInfo>

    protected fun mapTable(table: TableSchemaRow, detailLevel: DetailLevel, handleTableNameQualification: (FullyQualifiedTableName) -> FullyQualifiedTableName): Table {
        return Table(
            name = handleTableNameQualification(table.tableName),
            type = table.tableType,
            description = table.description,
            columns = table.columns?.map { mapColumn(it, table.tableType) },
            primary_key = table.pks,
            foreign_keys = (
                table.fks?.mapValues {
                    ForeignKeyConstraint(
                        handleTableNameQualification(it.value.foreign_table),
                        it.value.column_mapping
                    )
                }
                ),
            insertable = if (detailLevel == DetailLevel.BASIC_INFO) null else capabilities.mutations?.insert != null && table.tableType != TableType.VIEW,
            updatable = if (detailLevel == DetailLevel.BASIC_INFO) null else capabilities.mutations?.update != null && table.tableType != TableType.VIEW,
            deletable = if (detailLevel == DetailLevel.BASIC_INFO) null else capabilities.mutations?.delete != null && table.tableType != TableType.VIEW
        )
    }

    protected fun mapColumn(column: ColumnSchemaRow, tableType: TableType): Column {
        val generated = if (column.auto_increment) ColumnValueGenerationStrategy.AutoIncrement else null
        val insertable = capabilities.mutations?.insert != null && tableType != TableType.VIEW
        val updatable = capabilities.mutations?.update != null && tableType != TableType.VIEW

        return Column(
            name = column.name,
            type = mapScalarType(column.type, column.numeric_scale),
            description = column.description,
            nullable = column.nullable,
            value_generated = generated,
            insertable = insertable,
            updatable = updatable
        )
    }

    abstract fun mapScalarType(columnTypeStr: String, numericScale: Int?): ScalarType

    protected open fun getHandleTableNameQualification(database: String?, schema: String?, fullyQualifyTableNames: Boolean): (FullyQualifiedTableName) -> FullyQualifiedTableName {
        if (fullyQualifyTableNames) {
            return { fqName -> fqName }
        }

        return { fqName -> // [database, schema, <table name>] -> [<table name>]
            if (fqName.value.size == 3 && fqName.value[0] == database && fqName.value[1] == schema) {
                FullyQualifiedTableName(fqName.value[2])
            }
            // [database, <other schema>, <table name>] -> [<other schema>, <table name>]
            else if (fqName.value.size == 3 && fqName.value[0] == database) {
                FullyQualifiedTableName(fqName.value[1], fqName.value[2])
            }
            // [schema, <table name>] -> [<table name>]
            else if (fqName.value.size == 2 && fqName.value[0] == schema) {
                FullyQualifiedTableName(fqName.value[1])
            } else {
                fqName
            }
        }
    }

    protected open fun fullyQualifyTableName(database: String?, schema: String?, tableName: FullyQualifiedTableName): FullyQualifiedTableName {
        // [<database>, <schema>, <table name>] -> [<database>, <schema>, <table name>]
        if (tableName.value.size == 3) {
            return tableName
        }
        // [<schema>, <table name>] -> [database, <schema>, <table name>]
        else if (tableName.value.size == 2 && database != null) {
            return FullyQualifiedTableName(database, tableName.value[0], tableName.value[1])
        }
        // [<table name>] -> [database, schema, <table name>]
        else if (tableName.value.size == 1 && database != null && schema != null) {
            return FullyQualifiedTableName(database, schema, tableName.value[0])
        }
        // [<table name>] -> [schema, <table name>]
        else if (tableName.value.size == 1 && database == null && schema != null) {
            return FullyQualifiedTableName(schema, tableName.value[0])
        } else {
            return tableName
        }
    }

    protected fun stripFullQualification(name: FullyQualifiedTableName): FullyQualifiedTableName {
        return FullyQualifiedTableName(name.value.last())
    }
}
