package gdc.sqlgen

import gdc.sqlgen.custom.OracleGenerator
import gdc.sqlgen.custom.OracleMutationTranslator
import gdc.sqlgen.generic.*
import org.jooq.SQLDialect

object SqlGeneratorFactory {
    fun getSqlGenerator(dialect: SQLDialect): BaseQueryGenerator {
        return when (dialect) {
            SQLDialect.ORACLE -> OracleGenerator
            else -> throw Error("Unsupported database")
        }
    }

    fun getMutationTranslator(dialect: SQLDialect): BaseMutationTranslator {
        return when(dialect) {
            SQLDialect.ORACLE -> OracleMutationTranslator
            else -> throw NotImplementedError("Mutation not supported for this data source")
        }
    }
}
