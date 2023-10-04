package gdc.ir

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.annotation.JsonValue

/**
 * Describes the level of transactional atomicity the agent supports for mutation operations.
 * 'row': If multiple rows are affected in a single operation but one fails, only the failed row's changes will be reverted
 * 'single_operation': If multiple rows are affected in a single operation but one fails, all affected rows in the operation will be reverted
 * 'homogeneous_operations': If multiple operations of only the same type exist in the one mutation request, a failure in one will result in all changes being reverted
 * 'heterogeneous_operations': If multiple operations of any type exist in the one mutation request, a failure in one will result in all changes being reverted
 *
 */
enum class AtomicitySupportLevel {
    @JsonProperty("row")
    ROW,

    @JsonProperty("single_operation")
    SINGLE_OPERATION,

    @JsonProperty("homogeneous_operations")
    HOMOGENEOUS_OPERATIONS,

    @JsonProperty("heterogeneous_operations")
    HETEROGENEOUS_OPERATIONS
}

typealias InsertFieldValue = Any?

data class RowObject
@JsonCreator(mode = JsonCreator.Mode.DELEGATING)
constructor(@JsonValue val value: Map<String, InsertFieldValue>)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
sealed interface RowUpdate {

    val value: Any
    val value_type: ScalarType

    @JsonTypeName("increment")
    data class IncrementalColumnRowUpdate(
        val column: String,
        override val value: Any,
        override val value_type: ScalarType
    ) : RowUpdate

    @JsonTypeName("set")
    data class SetColumnRowUpdate(
        val column: String,
        override val value: Any,
        override val value_type: ScalarType
    ) : RowUpdate

    @JsonTypeName("custom_operator")
    data class CustomOperatorRowUpdate(
        val column: String,
        val operator_name: String, /* UpdateColumnOperatorName */
        override val value: Any,
        override val value_type: ScalarType
    ) : RowUpdate
}

data class MutationRequest(
    val insert_schema: List<TableInsertSchema>,
    val operations: List<MutationOperation>,
    val relationships: List<Relationship>
)

data class MutationResponse(
    val operation_results: List<MutationOperationResult> = emptyList()
)

typealias FieldName = String

data class MutationOperationResult(
    val affected_rows: Int,
    val returning: List<Map<FieldName, Any?>>? = null,
)

data class TableInsertSchema(
    val fields: Map<String, InsertFieldSchema>,
    val primary_key: List<ColumnName>? = null,
    val table: FullyQualifiedTableName
)

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
sealed interface InsertFieldSchema {

    @JsonTypeName("object_relation")
    data class ObjectRelationInsertSchema(
        val relationship: String,
        val insertion_order: ObjectRelationInsertionOrder,
    ) : InsertFieldSchema

    @JsonTypeName("array_relation")
    data class ArrayRelationInsertSchema(
        val relationship: String,
    ) : InsertFieldSchema

    @JsonTypeName("column")
    data class ColumnInsertSchema(
        val column: String,
        val column_type: ScalarType,
        val nullable: Boolean,
        val value_generated: ColumnValueGenerationStrategy?,
    ) : InsertFieldSchema

}

enum class ObjectRelationInsertionOrder {
    @JsonProperty("before_parent")
    BEFORE_PARENT,

    @JsonProperty("after_parent")
    AFTER_PARENT
}


interface HasReturningFields {
    val returning_fields: Map<String, Field>?
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
sealed interface MutationOperation {

    @JsonTypeName("insert")
    data class InsertMutationOperation(
        val table: FullyQualifiedTableName,
        val rows: List<RowObject>,
        val post_insert_check: Expression? = null,
        override val returning_fields: Map<String, Field>? = null,
    ) : MutationOperation, HasReturningFields

    @JsonTypeName("update")
    data class UpdateMutationOperation(
        val table: FullyQualifiedTableName,
        val updates: List<RowUpdate>,
        val where: Expression? = null,
        val post_update_check: Expression? = null,
        override val returning_fields: Map<String, Field>? = null,
    ) : MutationOperation, HasReturningFields

    @JsonTypeName("delete")
    data class DeleteMutationOperation(
        val table: FullyQualifiedTableName,
        val where: Expression? = null,
        override val returning_fields: Map<String, Field>? = null,
    ) : MutationOperation, HasReturningFields
}
