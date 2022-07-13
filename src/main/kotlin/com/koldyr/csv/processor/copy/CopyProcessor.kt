package com.koldyr.csv.processor.copy

import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.Callable
import kotlin.math.ceil
import kotlin.math.min
import kotlin.math.roundToLong
import org.slf4j.LoggerFactory
import com.koldyr.csv.Constants.FETCH_SIZE
import com.koldyr.csv.Constants.PARALLEL_PAGES
import com.koldyr.csv.db.SQLStatementFactory
import com.koldyr.csv.io.DbToDbPipeline
import com.koldyr.csv.model.PageBlockData
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import com.koldyr.csv.processor.BatchDBProcessor
import com.koldyr.util.executeWithTimer

/**
 * Description of class CopyProcessor
 *
 * @created: 2018.03.18
 */
class CopyProcessor(context: ProcessorContext) : BatchDBProcessor(context) {

    override fun processTable(tableName: String) {
        Thread.currentThread().name = tableName

        var srcConnection: Connection? = null
        var dstConnection: Connection? = null
        try {
            srcConnection = context[PoolType.SOURCE]
            dstConnection = context[PoolType.DESTINATION]
            val rowCount = getRowCount(srcConnection, tableName)

            executeWithTimer("table $tableName: ${format.format(rowCount)} rows") {
                val dataPipeline = DbToDbPipeline()

                if (rowCount > context.pageSize) {
                    release(srcConnection, PoolType.SOURCE)
                    release(dstConnection, PoolType.DESTINATION)
                    srcConnection = null //don't release this connection in finally block
                    dstConnection = null //don't release this connection in finally block

                    parallelCopy(dataPipeline, tableName, rowCount)
                } else {
                    copy(srcConnection!!, dstConnection!!, dataPipeline, tableName, rowCount)
                }
            }
        } catch (e: Exception) {
            LOGGER.error(e.message, e)
        } finally {
            release(srcConnection, PoolType.SOURCE)
            release(dstConnection, PoolType.DESTINATION)
        }
    }

    @Throws(Exception::class)
    private fun parallelCopy(dataPipeline: DbToDbPipeline, tableName: String, rowCount: Long) {
        val pageCount = ceil(rowCount / context.pageSize.toDouble()).toInt()

        LOGGER.debug("Copy {} pages", pageCount)

        val pages = ArrayList<PageBlockData>(pageCount)

        for (i in 0 until pageCount) {
            val start = i * context.pageSize
            val length = min(context.pageSize, rowCount - start)
            pages.add(PageBlockData(i, start, length))
        }

        context.setPages(tableName, pages)

        val columnCount: Int
        var connection: Connection? = null
        try {
            connection = context[PoolType.SOURCE]
            columnCount = getColumnCount(connection, tableName)
        } finally {
            context.release(PoolType.SOURCE, connection)
        }

        val sqlInsert = SQLStatementFactory.getInsertValues(connection!!, context.dstSchema, tableName, columnCount)

        val threadCount = min(PARALLEL_PAGES, pageCount)
        val copyThreads = ArrayList<Callable<Int>>(threadCount)
        for (i in 0 until threadCount) {
            copyThreads.add(PageCopyProcessor(context, tableName, dataPipeline, sqlInsert))
        }

        val results = context.executor.invokeAll(copyThreads)

        checkResults(tableName, pageCount, results)
    }

    @Throws(SQLException::class)
    private fun copy(srcConnection: Connection, dstConnection: Connection, dataPipeline: DbToDbPipeline, tableName: String, rowCount: Long) {
        val step = context.pageSize / 100.0

        val columnCount = getColumnCount(srcConnection, tableName)
        val insertSql = SQLStatementFactory.getInsertValues(dstConnection, context.dstSchema, tableName, columnCount)

        srcConnection.createStatement().use { srcStatement ->
            dstConnection.prepareStatement(insertSql).use { dstStatement ->
                val selectAll = SQLStatementFactory.getSelectAll(srcConnection, context.srcSchema, tableName)
                srcStatement.fetchSize = min(FETCH_SIZE.toLong(), rowCount).toInt()

                srcStatement.executeQuery(selectAll).use { resultSet ->
                    var counter = 0
                    while (dataPipeline.next(resultSet, dstStatement)) {
                        counter++

                        if (counter % step == 0.0) {
                            dstStatement.executeBatch()
                            dstConnection.commit()
                            val percent = (counter / rowCount.toDouble() * 100.0).roundToLong()
                            LOGGER.debug("\t{}%", percent)
                        }
                    }
                }

                dstStatement.executeBatch()
                dstConnection.commit()
            }
        }
    }

    @Throws(SQLException::class)
    private fun getColumnCount(connection: Connection, tableName: String): Int {
        try {
            connection.createStatement().use { statement ->
                val selectAll = SQLStatementFactory.getSelectAll(connection, context.srcSchema, tableName)
                statement.fetchSize = 1

                statement.executeQuery(selectAll).use {
                    return it.metaData.columnCount
                }
            }
        } finally {
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(CopyProcessor::class.java)
    }
}
