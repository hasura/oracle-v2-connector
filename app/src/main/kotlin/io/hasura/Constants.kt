package io.hasura

import gdc.ir.ApplyBinaryComparisonOperator
import gdc.ir.AtomicitySupportLevel
import gdc.ir.Capabilities
import gdc.ir.CapabilitiesResponse
import gdc.ir.ColumnNullability
import gdc.ir.ConfigSchema
import gdc.ir.DataSchemaCapabilities
import gdc.ir.GraphQLType
import gdc.ir.MutationCapabilities
import gdc.ir.QueryCapabilities
import gdc.ir.ReleaseName
import gdc.ir.ScalarType
import gdc.ir.ScalarTypeCapabilities
import gdc.ir.SingleColumnAggregateFunction
import io.hasura.models.DatasourceKind
import org.jooq.SQLDialect
import java.util.EnumMap

@JvmField
val JDBC_DATASOURCE_COMMON_CAPABILITY_RESPONSE = CapabilitiesResponse(
    capabilities = Capabilities(
        data_schema = DataSchemaCapabilities(
            supports_primary_keys = true,
            supports_foreign_keys = true,
            column_nullability = ColumnNullability.nullable_and_non_nullable
        ),
        post_schema = emptyMap(),
        queries = QueryCapabilities(
            foreach = emptyMap()
        ),
        scalar_types = mapOf(
            ScalarType.INT to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(
                    SingleColumnAggregateFunction.AVG to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.SUM to ScalarType.INT,
                    SingleColumnAggregateFunction.MIN to ScalarType.INT,
                    SingleColumnAggregateFunction.MAX to ScalarType.INT,
                    SingleColumnAggregateFunction.STDDEV_POP to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.STDDEV_SAMP to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.VAR_POP to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.VAR_SAMP to ScalarType.FLOAT
                ),
                graphql_type = GraphQLType.Int
            ),
            ScalarType.FLOAT to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(
                    SingleColumnAggregateFunction.AVG to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.SUM to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.MIN to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.MAX to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.STDDEV_POP to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.STDDEV_SAMP to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.VAR_POP to ScalarType.FLOAT,
                    SingleColumnAggregateFunction.VAR_SAMP to ScalarType.FLOAT
                ),
                graphql_type = GraphQLType.Float
            ),
            ScalarType.BOOLEAN to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(),
                graphql_type = GraphQLType.Boolean
            ),
            ScalarType.STRING to ScalarTypeCapabilities(
                comparison_operators = mapOf(
                    ApplyBinaryComparisonOperator.CONTAINS to ScalarType.STRING
                ),
                aggregate_functions = mapOf(
                    SingleColumnAggregateFunction.MIN to ScalarType.STRING,
                    SingleColumnAggregateFunction.MAX to ScalarType.STRING
                ),
                graphql_type = GraphQLType.String
            ),
            ScalarType.DATETIME to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(),
                graphql_type = GraphQLType.String
            ),
            ScalarType.DATETIME_WITH_TIMEZONE to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(),
                graphql_type = GraphQLType.String
            ),
            ScalarType.DATE to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(),
                graphql_type = GraphQLType.String
            ),
            ScalarType.TIME to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(),
                graphql_type = GraphQLType.String
            ),
            ScalarType.TIME_WITH_TIMEZONE to ScalarTypeCapabilities(
                comparison_operators = mapOf(),
                aggregate_functions = mapOf(),
                graphql_type = GraphQLType.String
            )
        ),
        relationships = emptyMap(),
        explain = emptyMap(),
        mutations = MutationCapabilities(
            atomicity_support_level = AtomicitySupportLevel.SINGLE_OPERATION,
            insert = mapOf(),
            update = mapOf(),
            delete = mapOf(),
            returning = mapOf()
        ),
        datasets = emptyMap(),
        licensing = emptyMap()
    ),
    config_schemas = ConfigSchema(
        config_schema = mapOf(
            "type" to "object",
            "nullable" to false,
            "properties" to mapOf(
                "jdbc_url" to mapOf(
                    "type" to "string",
                    "nullable" to false,
                    "title" to "JDBC Connection URL"
                ),
                "fully_qualify_all_names" to mapOf(
                    "type" to "boolean",
                    "nullable" to false,
                    "title" to "Fully qualify all your table names"
                )
            )
        )
    ),
    display_name = "GraphQL Data Connector",
    release_name = ReleaseName.Alpha
)
