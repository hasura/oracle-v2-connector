@file:Suppress("UnstableApiUsage")

package gdc.sqlgen.utils

import com.google.common.graph.MutableNetwork
import com.google.common.graph.NetworkBuilder
import gdc.ir.FullyQualifiedTableName
import gdc.ir.MutationRequest
import gdc.ir.QueryRequest
import gdc.ir.RelationshipEntry
import gdc.ir.RelationshipName
import gdc.ir.TableNamePart


data class RelationshipEdge(
    val table: FullyQualifiedTableName,
    val relationshipName: String,
    val entry: RelationshipEntry
)

// Represents the relationship graph of a single operation request (Query/Mutation)
interface IOperationRequestRelationGraph {
    fun getRelationsForTable(table: FullyQualifiedTableName): Set<RelationshipEdge>
    fun getIncomingRelationsForTable(table: FullyQualifiedTableName): Set<RelationshipEdge>
    fun getOutgoingRelationsForTable(table: FullyQualifiedTableName): Set<RelationshipEdge>
    fun getRelation(currentTable: FullyQualifiedTableName, relationName: RelationshipName): RelationshipEdge
    fun traverseRelEdges(path: List<TableNamePart>, startingTable: FullyQualifiedTableName): List<RelationshipEdge>
}

class OperationRequestRelationGraph(val tableRelationships: List<gdc.ir.Relationship>) :
    IOperationRequestRelationGraph {

    constructor(queryRequest: QueryRequest) : this(queryRequest.relationships)
    constructor(mutationRequest: MutationRequest) : this(mutationRequest.relationships)

    private val graph: MutableNetwork<FullyQualifiedTableName, RelationshipEdge> =
        NetworkBuilder.directed()
            .allowsSelfLoops(true)
            .allowsParallelEdges(true)
            .build()

    init {
        tableRelationships.forEach {
            it.relationships.forEach { (relName, relationship) ->
                graph.addEdge(
                    it.getName(),
                    relationship.target.getTargetName(),
                    RelationshipEdge(it.getName(), relName.value, relationship)
                )
            }
        }
    }

    override fun getRelationsForTable(table: FullyQualifiedTableName): Set<RelationshipEdge> = try {
        graph.incidentEdges(table)
    } catch (e: IllegalArgumentException) {
        emptySet()
    }

    override fun getIncomingRelationsForTable(table: FullyQualifiedTableName): Set<RelationshipEdge> = try {
        graph.inEdges(table)
    } catch (e: IllegalArgumentException) {
        emptySet()
    }

    override fun getOutgoingRelationsForTable(table: FullyQualifiedTableName): Set<RelationshipEdge> = try {
        graph.outEdges(table)
    } catch (e: IllegalArgumentException) {
        emptySet()
    }

    override fun getRelation(
        currentTable: FullyQualifiedTableName,
        relationName: RelationshipName
    ): RelationshipEdge {
        val t = getOutgoingRelationsForTable(currentTable)
        return t.first { it.relationshipName == relationName.value }
    }

    override fun traverseRelEdges(
        path: List<TableNamePart>,
        startingTable: FullyQualifiedTableName
    ): List<RelationshipEdge> {
        val edges = mutableListOf<RelationshipEdge>()
        var currentTable = startingTable
        path.forEach { tableNamePart ->
            val edge = getRelation(currentTable, RelationshipName(tableNamePart.value))
            edges.add(edge)
            currentTable = edge.entry.target.getTargetName()
        }
        return edges
    }
}
