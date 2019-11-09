package com.koldyr.csv.io

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types

/**
 * Pipeline to load data from one table to directly another database , w/o intermediate file
 *
 * @created: 2018.03.18
 */
class DbToDbPipeline : BaseDBPipeline() {

    @Throws(SQLException::class)
    fun next(source: ResultSet, destination: PreparedStatement): Boolean {
        if (source.next()) {
            try {
                copyValues(source, destination)
                destination.addBatch()
            } catch (e: Exception) {
                throw SQLException("Row num " + source.row, e)
            }

            return true
        }

        return false
    }

    @Throws(SQLException::class)
    private fun copyValues(source: ResultSet, destination: PreparedStatement) {
        val srcMetaData = source.metaData
        var columnCount = srcMetaData.columnCount

        if (hasNonDataColumns(source)) {
            columnCount--
        }

        for (columnIndex in 1..columnCount) {
            val columnType = getColumnType(srcMetaData, columnIndex)

            if (isString(columnType)) {
                val value = source.getString(columnIndex)
                destination.setString(columnIndex, value)
            } else if (isLOB(columnType)) {
                val lob = source.getBinaryStream(columnIndex)
                setLOB(destination, columnIndex, columnType, lob)
            } else if (columnType == Types.TIMESTAMP) {
                val timestamp = source.getTimestamp(columnIndex)
                destination.setTimestamp(columnIndex, timestamp)
            } else {
                val value = source.getObject(columnIndex)
                destination.setObject(columnIndex, value, columnType)
            }
        }
    }

    private fun hasNonDataColumns(resultSet: ResultSet): Boolean {
        return try {
            resultSet.findColumn("RNUM") > -1
        } catch (e: SQLException) {
            false
        }
    }
}
