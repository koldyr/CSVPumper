package com.koldyr.csv.processor.export

import com.koldyr.csv.Constants.FETCH_SIZE
import com.koldyr.csv.Constants.PARALLEL_PAGES
import com.koldyr.csv.db.SQLStatementFactory
import com.koldyr.csv.io.DBToFilePipeline
import com.koldyr.csv.model.PageBlockData
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import com.koldyr.csv.processor.BatchDBProcessor
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.Callable
import kotlin.math.ceil
import kotlin.math.min
import kotlin.math.roundToLong

/**
 * Description of class ExportProcessor
 *
 * @created: 2018.03.03
 */
class ExportProcessor(context: ProcessorContext) : BatchDBProcessor(context) {

    override fun processTable(tableName: String) {
        val start = System.currentTimeMillis()
        Thread.currentThread().name = tableName

        val csvFile = Path.of(context.path, "$tableName.csv")

        var connection: Connection? = null
        try {
            DBToFilePipeline(csvFile).use { dataPipeline ->
                connection = context[PoolType.SOURCE]
                val rowCount = getRowCount(connection!!, tableName)

                LOGGER.debug("Starting table {}: {} rows", tableName, format.format(rowCount))

                if (rowCount > context.pageSize) {
                    release(connection, PoolType.SOURCE)
                    connection = null //don't release this connection in finally block

                    parallelExport(dataPipeline, tableName, rowCount)
                } else {
                    export(connection!!, dataPipeline, tableName, rowCount.toDouble())
                }

                if (LOGGER.isDebugEnabled) {
                    val count = format.format(rowCount)
                    val duration = format.format(System.currentTimeMillis() - start)
                    LOGGER.debug("Finished table {}: {} rows in {} ms", tableName, count, duration)
                }
            }
        } catch (e: Exception) {
            LOGGER.error(e.message, e)
        } finally {
            release(connection, PoolType.SOURCE)
        }
    }

    @Throws(SQLException::class, IOException::class)
    private fun export(connection: Connection, dataPipeline: DBToFilePipeline, tableName: String, rowCount: Double) {
        val step = context.pageSize / 100.0

        connection.createStatement().use { statement ->
            val selectAll = SQLStatementFactory.getSelectAll(connection, context.srcSchema, tableName)
            statement.fetchSize = min(FETCH_SIZE.toDouble(), rowCount).toInt()

            statement.executeQuery(selectAll).use { resultSet ->
                val metaData = resultSet.metaData
                val columnCount = metaData.columnCount

                var counter = 0
                while (dataPipeline.next(resultSet, columnCount)) {
                    counter++

                    if (counter % step == 0.0) {
                        dataPipeline.flush()
                        val percent = (counter / rowCount * 100.0).roundToLong()
                        LOGGER.debug("\t{}%", percent)
                    }
                }
            }
        }
    }

    @Throws(InterruptedException::class)
    private fun parallelExport(dataPipeline: DBToFilePipeline, tableName: String, rowCount: Long) {
        val pageCount = ceil(rowCount / context.pageSize.toDouble()).toInt()

        LOGGER.debug("Export {} pages", pageCount)

        val pages = ArrayList<PageBlockData>(pageCount)

        for (i in 0 until pageCount) {
            val start = i * context.pageSize
            val length = min(context.pageSize, rowCount - start)
            pages.add(PageBlockData(i, start, length))
        }

        context.setPages(tableName, pages)

        val threadCount = min(PARALLEL_PAGES, pageCount)
        val exportThreads = ArrayList<Callable<Int>>(threadCount)
        for (i in 0 until threadCount) {
            exportThreads.add(PageExportProcessor(context, tableName, dataPipeline))
        }

        val results = context.executor.invokeAll(exportThreads)

        checkResults(tableName, pageCount, results)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ExportProcessor::class.java)
    }
}
