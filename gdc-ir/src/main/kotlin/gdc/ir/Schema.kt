@file:Suppress("ConstructorParameterNaming")

package gdc.ir

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.annotation.JsonValue

data class SchemaRequest(
    val filters: SchemaFilters? = null,
    val detail_level: DetailLevel = DetailLevel.EVERYTHING,
)

data class SchemaFilters(
    val only_tables: List<FullyQualifiedTableName>? = null,
    val only_functions: List<FullyQualifiedTableName>? = null,
)

enum class DetailLevel {
    @JsonProperty("everything")
    EVERYTHING,

    @JsonProperty("basic_info")
    BASIC_INFO
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class Schema(
    val tables: List<Table>,
    val functions: List<FunctionInfo>? = null,
)

enum class TableType {
    @JsonProperty("table")
    TABLE,

    @JsonProperty("view")
    VIEW
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class Table(
    val name: FullyQualifiedTableName,
    val type: TableType = TableType.TABLE,
    val columns: List<Column>? = null,
    val description: String? = null,
    val primary_key: List<ColumnName>? = null,
    val foreign_keys: Map<ConstraintName, ForeignKeyConstraint>? = null,
    val insertable: Boolean? = null,
    val updatable: Boolean? = null,
    val deletable: Boolean? = null,
)

data class ConstraintName
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: String) {
    init {
        require(value.isNotBlank()) { "Constraint name cannot be blank" }
    }
}

data class ForeignKeyConstraint(
    val foreign_table: FullyQualifiedTableName,
    val column_mapping: Map<SourceColumnName, TargetColumnName>,
)

data class Column(
    val name: String,
    val type: ScalarType,
    val nullable: Boolean,
    val description: String? = null,
    val insertable: Boolean = true,
    val updatable: Boolean = true,
    val value_generated: ColumnValueGenerationStrategy? = null
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface ColumnValueGenerationStrategy {

    @JsonTypeName("auto_increment")
    object AutoIncrement : ColumnValueGenerationStrategy

    @JsonTypeName("unique_identifier")
    object UniqueIdentifier : ColumnValueGenerationStrategy

    @JsonTypeName("default_value")
    object DefaultValue : ColumnValueGenerationStrategy
}


enum class ScalarType {
    @JsonProperty("Int")
    INT,

    @JsonProperty("Float")
    FLOAT,

    @JsonProperty("Boolean")
    BOOLEAN,

    @JsonProperty("String")
    @JsonAlias("string")
    STRING,

    @JsonProperty("datetime")
    DATETIME,

    @JsonProperty("datetime_with_timezone")
    DATETIME_WITH_TIMEZONE,

    @JsonProperty("date")
    DATE,

    @JsonProperty("time")
    TIME,

    @JsonProperty("time_with_timezone")
    TIME_WITH_TIMEZONE,

    @JsonProperty("number")
    NUMBER,
}
