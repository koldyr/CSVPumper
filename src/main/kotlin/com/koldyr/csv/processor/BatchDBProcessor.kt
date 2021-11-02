package com.koldyr.csv.processor

import com.koldyr.csv.db.SQLStatementFactory
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
import java.util.concurrent.Callable
import java.util.concurrent.Future

/**
 * Description of class DBProcessor
 *
 * @created: 2018.03.03
 */
abstract class BatchDBProcessor
    protected constructor(protected val context: ProcessorContext) : Callable<Any> {

    protected val format: DecimalFormat

    init {
        val decimalFormatSymbols = DecimalFormatSymbols()
        decimalFormatSymbols.groupingSeparator = ','
        format = DecimalFormat("###,###,###,###", decimalFormatSymbols)
    }

    override fun call(): Any? {
        var tableName: String? = context.nextTable
        while (tableName != null) {
            processTable(tableName)
            tableName = context.nextTable
        }

        return null
    }

    protected abstract fun processTable(tableName: String)

    @Throws(InterruptedException::class)
    protected fun checkResults(tableName: String, pageCount: Int, results: Collection<Future<Int>>) {
        val count = results.stream().mapToInt { result ->
            return@mapToInt try {
                result.get()
            } catch (e: Exception) {
                0
            }
        }.sum()

        LoggerFactory.getLogger(javaClass).debug("checkResults - Expected: {}, Actual: {}", pageCount, count)

        if (count != pageCount) {
            throw InterruptedException("Error exporting $tableName")
        }
    }

    @Throws(SQLException::class)
    protected fun getRowCount(connection: Connection, tableName: String): Long {
        var rowCount: Long = 0

        connection.createStatement().use { statement ->
            val getRowCount = SQLStatementFactory.getRowCount(connection, context.srcSchema, tableName)
            statement.executeQuery(getRowCount).use {
                if (it.next()) {
                    rowCount = it.getLong(1)
                }
            }
        }
        return rowCount
    }

    protected fun release(connection: Connection?, type: PoolType) {
        if (connection != null) {
            try {
                context.release(type, connection)
            } catch (e: Exception) {
                LoggerFactory.getLogger(javaClass).error(e.message, e)
            }
        }
    }
}
