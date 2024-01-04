@file:Suppress("ConstructorParameterNaming")

package gdc.ir

import com.fasterxml.jackson.annotation.*
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

data class Alias
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: String) {
    init {
        require(value.isNotEmpty()) { "Alias value must not be empty" }
        require(value.all { it.isLetterOrDigit() || it == '_' }) { "Alias value must be alphanumeric" }
    }
}

/**
 * A "TableNamePart" is one segment of a Fully-Qualified Table Name (FQTN).
 * IE: "public"."my_table" is a FQTN, and "public" and "my_table" are TableNameParts.
 */
data class TableNamePart
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: String) {
    init {
        require(value.isNotBlank()) { "Table name part cannot be blank" }
        require(value.all { it.isLetterOrDigit() || it == '_' }) { "Table name part must be alphanumeric" }
    }
}

/**
 * ColumnName is the leaf/terminal node of a FQCN.
 */
data class ColumnName
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: String) {
    init {
        require(value.isNotBlank()) { "Column name cannot be blank" }
        require(value.all { it.isLetterOrDigit() || it == '_' }) { "Column name must be alphanumeric" }
    }
}

data class RelationshipName
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: String) {
    init {
        require(value.isNotBlank()) { "Relationship name cannot be blank" }
    }
}

data class SourceColumnName
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: String) {
    init {
        require(value.isNotBlank()) { "Source column name cannot be blank" }
        require(value.all { it.isLetterOrDigit() || it == '_' }) { "Source column name must be alphanumeric" }
    }
}

data class TargetColumnName
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: String) {
    init {
        require(value.isNotBlank()) { "Target column name cannot be blank" }
        require(value.all { it.isLetterOrDigit() || it == '_' }) { "Target column name must be alphanumeric" }
    }
}

// TODO: Replace List<String> with List<TableName>
// This will require update of the SQL generation code
data class FullyQualifiedTableName
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: List<String>) {
    constructor(vararg parts: String) : this(parts.toList())

    init {
        require(value.isNotEmpty()) { "Fully qualified table name cannot be empty" }
    }

    val tableName: String = value.last()
}

interface HasColumnName {
    val column: ColumnName
}

interface HasColumnType {
    val column_type: String
}


// /////////////////////////////////////////////////////////////////////////
// QUERY
// /////////////////////////////////////////////////////////////////////////

// Used in the "foreach" clause
data class ScalarValue(
    val value: Any,
    val value_type: ScalarType,
)

data class QueryRequest(
    val query: Query,
    val target: Target,
    val relationships: List<Relationship> = emptyList(),
    val foreach: List<Map<String, ScalarValue>>? = null,
    @JsonProperty("interpolated_queries")
    val interpolatedQueries: Map<String, InterpolatedQuery>? = null
){
    fun getName(): FullyQualifiedTableName = target.getTargetName()
    fun targetName(): String = getName().value.last()
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface Target {

   fun getTargetName(): FullyQualifiedTableName
    @JsonTypeName("table")
    data class TableTarget(val name: FullyQualifiedTableName) : Target {
        override fun getTargetName() = this.name
    }

    @JsonTypeName("function")
    data class FunctionTarget(
        val name: FullyQualifiedTableName,
        val arguments: List<FunctionRequestFunctionArgument>? = null
    ) : Target {
        override fun getTargetName() = this.name
    }

    @JsonTypeName("interpolated")
    data class InterpolatedTarget(val id: String): Target {
        override fun getTargetName() = FullyQualifiedTableName(id)
    }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface FunctionRequestFunctionArgument {
    @JsonTypeName("named")
    data class Named(val name: String, val value: ArgumentValue) : FunctionRequestFunctionArgument
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface ArgumentValue {
    @JsonTypeName("scalar")
    data class Scalar(val value: Any, val value_type: ScalarType) : ArgumentValue
}


@JsonInclude(JsonInclude.Include.NON_NULL)
data class FunctionArgument(
    val name: String,
    val type: ScalarType,
    val optional: Boolean? = null,
)

enum class ReadWriteMode {
    @JsonProperty("read")
    READ,

    @JsonProperty("write")
    WRITE,
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class FunctionInfo(
    val name: FullyQualifiedTableName,
    val type: ReadWriteMode,
    val returns: FunctionReturnType?,
    val response_cardinality: FunctionResponseCardinality?,
    val args: List<FunctionArgument>?,
    val description: String? = null,
)

enum class FunctionResponseCardinality {
    @JsonProperty("one")
    ONE,

    @JsonProperty("many")
    MANY,
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface FunctionReturnType {
    @JsonTypeName("table")
    data class Table(val table: FullyQualifiedTableName) : FunctionReturnType

    @JsonTypeName("unknown")
    object Unknown : FunctionReturnType
}

@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
sealed interface Relationship {
    val relationships: Map<RelationshipName, RelationshipEntry>
    fun getName(): FullyQualifiedTableName
}

data class TableRelationship(
    val source_table: FullyQualifiedTableName,
    override val relationships: Map<RelationshipName, RelationshipEntry>,
) : Relationship {
    override fun getName(): FullyQualifiedTableName = source_table
}

data class FunctionRelationship(
    val source_function: FullyQualifiedTableName,
    override val relationships: Map<RelationshipName, RelationshipEntry>,
) : Relationship {
    override fun getName(): FullyQualifiedTableName = source_function
}

data class InterpolatedQueryRelationship(
    val source_interpolated_query: String,
    override val relationships: Map<RelationshipName, RelationshipEntry>,
) : Relationship {
    override fun getName(): FullyQualifiedTableName = FullyQualifiedTableName(source_interpolated_query)
}


data class RelationshipEntry(
    val target: Target,
    val relationship_type: RelationshipType,
    val column_mapping: Map<SourceColumnName, TargetColumnName>,
)

enum class RelationshipType {
    @JsonProperty("object")
    OBJECT,

    @JsonProperty("array")
    ARRAY,
}


data class InterpolatedQuery (
    val id: String,
    val items: List<InterpolatedItem>
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface InterpolatedItem {
    @JsonTypeName("text")
    data class InterpolatedText(val value: String): InterpolatedItem

    @JsonTypeName("scalar")
    data class InterpolatedScalar(val value: Any, val value_type: ScalarType): InterpolatedItem
}



data class Query(
    val aggregates: Map<Alias, Aggregate>? = null,
    val fields: Map<Alias, Field>? = null,
    val limit: Int? = null,
    val offset: Int? = null,
    val where: Expression? = null,
    val order_by: OrderBy? = null,
)

data class ColumnReference(
    val path: List<TableNamePart> = emptyList(),
    val name: String,
    override val column_type: String = "unknown",
) : HasColumnType

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface Aggregate {

    @JsonTypeName("star_count")
    object StarCount : Aggregate

    @JsonTypeName("column_count")
    data class ColumnCount(override val column: ColumnName, val distinct: Boolean) : Aggregate, HasColumnName

    @JsonTypeName("single_column")
    data class SingleColumn(override val column: ColumnName, val function: SingleColumnAggregateFunction) : Aggregate,
        HasColumnName
}

enum class SingleColumnAggregateFunction {
    @JsonProperty("avg")
    AVG,

    @JsonProperty("sum")
    SUM,

    @JsonProperty("min")
    MIN,

    @JsonProperty("max")
    MAX,

    @JsonProperty("stddev_pop")
    STDDEV_POP,

    @JsonProperty("stddev_samp")
    STDDEV_SAMP,

    @JsonProperty("var_pop")
    VAR_POP,

    @JsonProperty("var_samp")
    VAR_SAMP,
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface Field {

    @JsonTypeName("column")
    data class ColumnField(val column: String, override val column_type: String = "unknown") : Field, HasColumnType

    @JsonTypeName("relationship")
    data class RelationshipField(val relationship: RelationshipName, val query: Query) : Field
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface ComparisonValue {

    @JsonTypeName("column")
    data class ColumnValue(override val column: ColumnReference) : ComparisonValue, HasColumnReference

    @JsonTypeName("scalar")
    data class ScalarValue(val value: String, val value_type: String) : ComparisonValue
}

enum class OrderDirection {
    @JsonProperty("asc")
    ASC,

    @JsonProperty("desc")
    DESC
}

data class OrderBy(
    /**
     * The elements to order by, in priority order
     */
    val elements: List<OrderByElement>,
    /**
     * A map of relationships from the current query table to target tables.
     * The key of the map is the relationship name. The relationships are used within the order by elements.
     */
    val relations: Map<RelationshipName, OrderByRelation> = emptyMap(),
)

data class OrderByRelation(
    /**
     * Further relationships to follow from the relationship's target table.
     * The key of the map is the relationship name.
     */
    val subrelations: Map<RelationshipName, OrderByRelation>,
    val where: Expression? = null,
)

data class OrderByElement(
    val target: OrderByTarget,
    /**
     * The relationship path from the current query table to the table that contains the target to order by.
     * This is always non-empty for aggregate order by targets
     */
    val target_path: List<TableNamePart>,
    val order_direction: OrderDirection
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface OrderByTarget {

    @JsonTypeName("star_count_aggregate")
    object OrderByStarCountAggregate : OrderByTarget

    @JsonTypeName("column")
    data class OrderByColumn(override val column: ColumnName) : OrderByTarget, HasColumnName

    @JsonTypeName("single_column_aggregate")
    data class OrderBySingleColumnAggregate(
        override val column: ColumnName,
        val function: SingleColumnAggregateFunction
    ) : OrderByTarget, HasColumnName
}

// /////////////////////////////////////////////////////////////////////////
// OPERATORS
// /////////////////////////////////////////////////////////////////////////
enum class ApplyBinaryComparisonOperator {
    @JsonProperty("equal")
    EQUAL,

    @JsonProperty("greater_than")
    GREATER_THAN,

    @JsonProperty("greater_than_or_equal")
    GREATER_THAN_OR_EQUAL,

    @JsonProperty("less_than")
    LESS_THAN,

    @JsonProperty("less_than_or_equal")
    LESS_THAN_OR_EQUAL,

    @JsonProperty("_contains")
    CONTAINS,

    @JsonProperty("_like")
    LIKE,

    @JsonProperty("_ilike")
    LIKE_IGNORE_CASE,

    @JsonProperty("_regex")
    LIKE_REGEX
}

enum class ApplyUnaryComparisonOperator {
    @JsonProperty("is_null")
    IS_NULL
}

enum class ApplyBinaryArrayComparisonOperator {
    @JsonProperty("in")
    IN
}

// /////////////////////////////////////////////////////////////////////////
// EXPRESSIONS
// /////////////////////////////////////////////////////////////////////////
interface HasColumnReference {
    val column: ColumnReference
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface Expression {

    @JsonTypeName("and")
    data class And(val expressions: List<Expression>) : Expression

    @JsonTypeName("or")
    data class Or(val expressions: List<Expression>) : Expression

    @JsonTypeName("not")
    data class Not(val expression: Expression) : Expression

    @JsonTypeName("binary_op")
    data class ApplyBinaryComparison(
        val operator: ApplyBinaryComparisonOperator,
        override val column: ColumnReference,
        val value: ComparisonValue
    ) : Expression, HasColumnReference

    @JsonTypeName("binary_arr_op")
    data class ApplyBinaryArrayComparison(
        val operator: ApplyBinaryArrayComparisonOperator,
        override val column: ColumnReference,
        val values: List<*>
    ) : Expression, HasColumnReference

    @JsonTypeName("unary_op")
    data class ApplyUnaryComparison(
        val operator: ApplyUnaryComparisonOperator,
        override val column: ColumnReference
    ) : Expression, HasColumnReference

    // Test if a row exists that matches the where subexpression in the specified table (in_table)
    @JsonTypeName("exists")
    data class Exists(
        val in_table: ExistsInTable,
        val where: Expression
    ) : Expression
}

// The value of the in_table property of the exists expression is an object that describes which table to look for rows in. The object is tagged with a type property:
//
// | type      | Additional fields | Description                                                                                                                                                              |
// | --------- | ----------------- |
// | related   | relationship      | The table is related to the current table via the relationship name specified in relationship (this means it should be joined to the current table via the relationship) |
// | unrelated | table             | The table specified by table is unrelated to the current table and therefore is not explicitly joined to the current table                                               |
//
// The "current table" during expression evaluation is the table specified by the closest ancestor exists expression, or if there is no exists ancestor, it is the table involved in the Query that the whole where Expression is from.
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
sealed interface ExistsInTable {

    @JsonTypeName("related")
    data class RelatedTable(val relationship: RelationshipName) : ExistsInTable

    @JsonTypeName("unrelated")
    data class UnrelatedTable(val table: FullyQualifiedTableName) : ExistsInTable
}

data class QueryResult(
    /**
     * Aggregates must be "null" when empty for the GDC Agent Testkit to work
     * (it fails if empty object "{}" is returned)
     */
    val aggregates: Map<String, Any>? = null,
    val rows: List<Map<String, Any?>>? = emptyList()
)
