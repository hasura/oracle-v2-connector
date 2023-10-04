package gdc.ir

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

enum class ReleaseName {
    Alpha,
    Beta,

    @JsonProperty("")
    GA
}

data class CapabilitiesResponse(
    val capabilities: Capabilities,
    val config_schemas: ConfigSchema,
    val display_name: String,
    val release_name: ReleaseName
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class QueryCapabilities(
    val foreach: Map<String, Any>? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class Capabilities(
    val data_schema: DataSchemaCapabilities,
    val post_schema: Map<String, Any>? = mapOf(),
    val queries: QueryCapabilities = QueryCapabilities(),
    val mutations: MutationCapabilities? = null,
    val scalar_types: Map<ScalarType, ScalarTypeCapabilities> = mapOf(),
    val relationships: Map<String, Any>?,
    val explain: Map<String, Any>,
    val datasets: Map<String, Any>? = null,
    val licensing: Map<String, Any>? = null,
    val user_defined_functions: Map<String, Any>? = null,
    val interpolated_queries: Map<String, Any>? = null,
)

data class DataSchemaCapabilities(
    val supports_primary_keys: Boolean,
    val supports_foreign_keys: Boolean,
    val column_nullability: ColumnNullability
)

enum class ColumnNullability {
    only_nullable,
    nullable_and_non_nullable
}

data class ScalarTypeCapabilities(
    val comparison_operators: Map<ApplyBinaryComparisonOperator, ScalarType> = mapOf(),
    val aggregate_functions: Map<SingleColumnAggregateFunction, ScalarType> = mapOf(),
    val graphql_type: GraphQLType? = null
)

typealias DeleteCapabilities = Map<String, Any>
typealias InsertCapabilities = Map<String, Any>
typealias UpdateCapabilities = Map<String, Any>
typealias ReturningCapabilities = Map<String, Any>

data class MutationCapabilities(
    val atomicity_support_level: AtomicitySupportLevel? = null,
    val delete: DeleteCapabilities? = null,
    val insert: InsertCapabilities? = null,
    val update: UpdateCapabilities? = null,
    val returning: ReturningCapabilities? = null
)

enum class GraphQLType {
    Int,
    Float,
    String,
    Boolean,
    ID
}

data class ConfigSchema(
    val config_schema: Map<String, Any> = mapOf(),
    val other_schemas: Map<String, Any> = mapOf()
)
