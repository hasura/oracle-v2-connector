package gdc.sqlgen.utils


// Translation of the C# implementation of the "NestHydration" algorithm
// which converts tabular data produced by JOIN queries into nested objects.
//
// SEE: https://github.com/CoursePark/NestHydrationJS#sql-ish-example
// SEE: https://github.com/umbraco/nest-hydration-dotnet
//
// This allows you to make a query like:
//
// SELECT
//      artist.id as id,
//      artist.name as name,
//      album.id as album_id,
//      album.title as album_title
//  FROM
//      artist JOIN album ON artist.id = album.artist_id
//
// And convert the flat, tabular rows into a nested object like:
//
// {
//      id: 1,
//      name: "Artist 1",
//      albums: [
//          {
//              id: 1,
//              title: "Album 1"
//          }
//      ]
// }
//
// This is done by providing a column-mapping definition object, like:
//
// Definition(listOf(
//      Property("id", id=true),
//      Property("name"),
//      PropertyArray("albums", listOf(
//          Property(name="id", column="album_id", id=true),
//          Property(name="title", column="album_title")
//      ))
// ))

sealed interface IProperty {
    val name: String
}

data class Definition(val properties: List<IProperty>)

/**
 * A single property in a nested object.
 * IE: { "id": 1 }
 */
data class Property(
    override val name: String,
    val column: String,
    val isId: Boolean = false
) : IProperty

/**
 * An array of [IProperty] objects.
 * IE: { "albums": [ { "id": 1, "title": "Album 1", "genre": { "genre_id": 1 } } ] }
 */
data class PropertyArray(
    override val name: String,
    val properties: List<IProperty>,
    /**
     * Unwrap handles extracting single-item arrays into an object
     *
     * It is used to bend the NestHydration algorithm output to the
     * shape required in [gdc.ir.QueryResult.rows] during [mkQueryNestSchema]
     **/
    val unwrap: Boolean = false
) : IProperty

/**
 * A nested object containing [IProperty] objects.
 * IE: { "artist": { "id": 1, "name": "Artist 1", "albums": [ { "id": 1, "title": "Album 1" } ] } }
 */
data class PropertyObject(
    override val name: String,
    val properties: List<IProperty>
) : IProperty


typealias Result = MutableMap<String, Any?>
typealias Row = MutableMap<String, Any?>

object Hydrator {


    // These are set to NULL instead of emptyMap()/emptyList()
    // because the GDC reference agent seems to return null for these
    private val EMPTY_LIST_VALUE = mutableListOf<Any?>()
    private val EMPTY_OBJECT_VALUE = mutableMapOf<String, Any?>()

    // Algorithm description:
    // 1. It takes a list of maps (the data set) and a definition of the data structure.
    // 2. It loops through the data set, and for each row it builds a new entry in the result list.
    // 3. The entry is built by extracting the properties from the row and adding them to the entry.
    //  3a. If a property is an object, it will extract its properties from the row and add them to the entry.
    //  3b. If a property is an array, it will extract its properties from the row and add them to a new entry
    //      in the array.
    //  3c. If a property is an array, and there's already an entry in the array with the same ID as the current row,
    //      it will add its properties to that entry instead of creating a new one.
    //  3d. If any of these properties is an ID, and there's no value for it in the current row, it will skip that
    //      property altogether (and all of its children).
    fun nest(dataset: List<Row>, definition: Definition): List<Result> {
        val result = mutableListOf<Result>()

        val properties = definition.properties
        val primaryIdColumns = properties.filter { it is Property && it.isId } as List<Property>

        for (row in dataset) {
            val mappedEntry = buildEntry(primaryIdColumns, row, result) ?: continue

            for (property in properties) {
                when (property) {
                    is Property -> {
                        if (property.isId) continue
                        extract(property, row, mappedEntry)
                    }

                    is PropertyObject -> extractObject(property, row, mappedEntry)
                    is PropertyArray -> extractArray(property, row, mappedEntry)
                }
            }
        }

        return result
    }

    private fun extract(property: Property, row: Row, mappedEntry: Result) {
        mappedEntry[property.name] = row[property.column]
    }

    private fun extractObject(propertyObject: PropertyObject, row: Row, mappedEntry: Result) {
        val newEntry = mutableMapOf<String, Any?>()

        for (property in propertyObject.properties) {
            when (property) {
                is Property -> {
                    if (property.isId && row[property.column] == null) {
                        mappedEntry[propertyObject.name] = null
                        return
                    }
                    extract(property, row, newEntry)
                }

                is PropertyObject -> extractObject(property, row, newEntry)
                is PropertyArray -> extractArray(property, row, newEntry)
            }
        }

        mappedEntry[propertyObject.name] = if (newEntry.isEmpty()) EMPTY_OBJECT_VALUE else newEntry
    }

    private fun extractArray(propertyArray: PropertyArray, row: Row, mappedEntry: Result) {
        val primaryIdColumns = propertyArray.properties.filter { it is Property && it.isId } as List<Property>
        val entryExists = mappedEntry[propertyArray.name] != null

        val list = if (entryExists) {
            val entry = mappedEntry[propertyArray.name]
            if (entry is List<*>) entry else mutableListOf(entry)
        } else {
            mutableListOf()
        }

        val mapped = buildEntry(primaryIdColumns, row, list as MutableList<Result>) ?: return

        for (property in propertyArray.properties) {
            when (property) {
                is Property -> {
                    if (property.isId) continue
                    extract(property, row, mapped)
                }

                is PropertyObject -> extractObject(property, row, mapped)
                is PropertyArray -> extractArray(property, row, mapped)
            }
        }

        if (!entryExists) {
            mappedEntry[propertyArray.name] = if (list.size == 1 && list[0].isEmpty())
                EMPTY_LIST_VALUE
            else
                if (propertyArray.unwrap) list.first() else list
        }
    }

    private fun buildEntry(primaryIdColumns: List<Property>, row: Row, result: MutableList<Result>): Result? {
        if (primaryIdColumns.any { row[it.column] == null }) return null

        val existing = result.find { primaryIdColumns.all { pkCol -> it[pkCol.name] == row[pkCol.column] } }
        if (existing != null) return existing

        val newEntry = primaryIdColumns.associateTo(mutableMapOf()) { it.name to row[it.column] }
        result.add(newEntry)
        return newEntry
    }

}
