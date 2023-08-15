package com.koldyr.csv.io

import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets.*
import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Types
import org.postgresql.core.Oid
import com.koldyr.csv.db.DatabaseDetector.isBLOBSupported
import com.koldyr.csv.db.DatabaseDetector.isCLOBSupported
import com.koldyr.csv.db.DatabaseDetector.isMsSQLServer
import com.koldyr.csv.db.DatabaseDetector.isPostgreSQL

/**
 * Description of class BaseDBPipeline
 *
 * @created: 2018.09.24
 */
abstract class BaseDBPipeline {
    protected fun isString(columnType: Int): Boolean {
        return columnType == Types.VARCHAR || columnType == Types.NVARCHAR || columnType == Types.NCHAR || columnType == Types.CHAR
    }

    protected fun isLOB(columnType: Int): Boolean {
        return (columnType == Types.BLOB || columnType == Types.CLOB || columnType == Types.NCLOB || columnType == Types.BINARY || columnType == Types.LONGVARBINARY
            || columnType == Oid.TEXT)
    }

    @Throws(SQLException::class)
    protected fun setLOB(destination: PreparedStatement, columnIndex: Int, columnType: Int, lob: InputStream) {
        when (columnType) {
            Types.BLOB -> {
                if (isBLOBSupported(destination)) {
                    destination.setBlob(columnIndex, lob)
                } else {
                    destination.setBinaryStream(columnIndex, lob)
                }
            }
            Types.CLOB -> {
                if (isCLOBSupported(destination)) {
                    destination.setClob(columnIndex, InputStreamReader(lob, UTF_8))
                } else {
                    destination.setCharacterStream(columnIndex, InputStreamReader(lob, UTF_8))
                }
            }
            Types.NCLOB -> {
                destination.setNClob(columnIndex, InputStreamReader(lob, UTF_8))
            }
            Types.BINARY, Types.LONGVARBINARY -> {
                destination.setBinaryStream(columnIndex, lob)
            }
            Oid.TEXT -> {
                destination.setCharacterStream(columnIndex, InputStreamReader(lob, UTF_8))
            }
        }
    }

    @Throws(SQLException::class)
    protected fun getColumnType(metaData: ResultSetMetaData, columnIndex: Int): Int {
        var columnType = metaData.getColumnType(columnIndex)

        if (isString(columnType)) {
            if (isPostgreSQL(metaData)) {
                val columnTypeName = metaData.getColumnTypeName(columnIndex)
                if (columnTypeName == "text") {
                    columnType = Oid.TEXT
                }
            } else if (isMsSQLServer(metaData)) {
                val precision = metaData.getPrecision(columnIndex)
                if (columnType == Types.VARCHAR) {
                    if (precision == 2_147_483_647) {
                        return Types.CLOB
                    }
                } else if (columnType == Types.NVARCHAR) {
                    if (precision == 1_073_741_823) {
                        return Types.NCLOB
                    }
                }
            }
        }
        return columnType
    }
}
