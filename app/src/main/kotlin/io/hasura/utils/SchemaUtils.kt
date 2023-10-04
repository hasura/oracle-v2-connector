package io.hasura.utils

import gdc.ir.ScalarType

fun javaSqlTypeToGDCScalar(sqlType: Int): ScalarType {
    return when (sqlType) {
        java.sql.Types.BIT -> ScalarType.BOOLEAN
        java.sql.Types.BOOLEAN -> ScalarType.BOOLEAN

        java.sql.Types.TINYINT -> ScalarType.INT
        java.sql.Types.SMALLINT -> ScalarType.INT
        java.sql.Types.INTEGER -> ScalarType.INT
        java.sql.Types.BIGINT -> ScalarType.INT

        java.sql.Types.FLOAT -> ScalarType.FLOAT
        java.sql.Types.REAL -> ScalarType.FLOAT
        java.sql.Types.DOUBLE -> ScalarType.FLOAT
        java.sql.Types.NUMERIC -> ScalarType.FLOAT
        java.sql.Types.DECIMAL -> ScalarType.FLOAT

        java.sql.Types.CHAR -> ScalarType.STRING
        java.sql.Types.VARCHAR -> ScalarType.STRING
        java.sql.Types.LONGVARCHAR -> ScalarType.STRING
        java.sql.Types.NCHAR -> ScalarType.STRING
        java.sql.Types.NVARCHAR -> ScalarType.STRING
        java.sql.Types.LONGNVARCHAR -> ScalarType.STRING

        java.sql.Types.DATE -> ScalarType.DATE
        java.sql.Types.TIME -> ScalarType.TIME
        java.sql.Types.TIME_WITH_TIMEZONE -> ScalarType.TIME_WITH_TIMEZONE
        java.sql.Types.TIMESTAMP -> ScalarType.DATETIME
        java.sql.Types.TIMESTAMP_WITH_TIMEZONE -> ScalarType.DATETIME_WITH_TIMEZONE

        java.sql.Types.OTHER -> ScalarType.STRING
        else -> ScalarType.STRING
    }
}
