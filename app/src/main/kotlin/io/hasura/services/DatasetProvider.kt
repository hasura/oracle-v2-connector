package io.hasura.services

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import jakarta.inject.Singleton
import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.support.EncodedResource
import org.springframework.jdbc.datasource.init.ScriptUtils
import java.sql.Connection
import java.time.OffsetDateTime

data class TableData(
    var tableName: String,
    var columns: Map<String, String>,
    var rows: List<Map<String, JsonNode>>
)

@Singleton
class DatasetProvider {
    private fun getSchemaResource(templateName: String, databaseType: String): ClassPathResource {
        return ClassPathResource("/dataset-templates/$templateName/schema-$databaseType.sql")
    }

    private fun getTableDataResource(templateName: String): ClassPathResource {
        return ClassPathResource("/dataset-templates/$templateName/table-data.json")
    }

    fun templateExists(templateName: String, databaseType: String): Boolean {
        return getSchemaResource(templateName, databaseType).exists()
    }

    fun createSchemaInClone(cloneConnection: Connection, templateName: String, databaseType: String) {
        val schemaResource = getSchemaResource(templateName, databaseType)
        ScriptUtils.executeSqlScript(cloneConnection, EncodedResource(schemaResource, "utf8"))
    }

    private fun insertTableData(connection: Connection, tableData: TableData) {
        val columns = tableData.columns.toList()
        val columnsFragment = columns.joinToString(",") { it.first }
        val parametersFragment = columns.joinToString(",") { "?" }
        val sql = "INSERT INTO ${tableData.tableName} ($columnsFragment) VALUES ($parametersFragment)"

        val preparedStatement = connection.prepareStatement(sql)

        for (rowBatch in tableData.rows.chunked(1000)) {
            for (row in rowBatch) {
                for (columnItem in columns.withIndex()) {
                    var parameterIndex = columnItem.index + 1
                    val columnValue = row.get(columnItem.value.first)

                    if (columnValue == null || columnValue.isNull) {
                        preparedStatement.setNull(parameterIndex, java.sql.Types.NULL)
                    } else {
                        when (columnItem.value.second) {
                            "string" -> preparedStatement.setString(parameterIndex, columnValue.textValue())
                            "number" -> {
                                if (columnValue.isFloatingPointNumber) {
                                    preparedStatement.setDouble(parameterIndex, columnValue.doubleValue())
                                } else if (columnValue.canConvertToInt()) {
                                    preparedStatement.setInt(parameterIndex, columnValue.intValue())
                                } else if (columnValue.canConvertToLong()) {
                                    preparedStatement.setLong(parameterIndex, columnValue.longValue())
                                }
                            }

                            "DateTime" -> {
                                val offsetDateTime = OffsetDateTime.parse(columnValue.textValue())
                                preparedStatement.setObject(parameterIndex, offsetDateTime)
                            }

                            "boolean" -> preparedStatement.setBoolean(parameterIndex, columnValue.booleanValue())
                            else -> throw Exception("Unknown column type: ${columnItem.value.second}")
                        }
                    }
                }
                preparedStatement.addBatch()
            }
            preparedStatement.executeBatch()
        }
    }

    fun insertDataIntoClone(cloneConnection: Connection, templateName: String) {
        val tableDataResource = getTableDataResource(templateName)
        val mapper = jacksonObjectMapper()
        val tables = mapper.readValue(tableDataResource.inputStream, Array<TableData>::class.java)

        tables.forEach { insertTableData(cloneConnection, it) }
    }
}
