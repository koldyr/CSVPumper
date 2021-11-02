package com.koldyr.csv.processor.export

import com.koldyr.csv.Constants.FETCH_SIZE
import com.koldyr.csv.db.SQLStatementFactory
import com.koldyr.csv.io.DBToFilePipeline
import com.koldyr.csv.model.PageBlockData
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import com.koldyr.csv.processor.BasePageProcessor
import com.koldyr.csv.processor.RetryCall
import org.slf4j.LoggerFactory
import java.io.IOException
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.Callable
import kotlin.math.min
import kotlin.math.roundToLong

/**
 * Description of class PageExportProcessor
 *
 * @created: 2018.03.05
 */
class PageExportProcessor(
        context: ProcessorContext,
        tableName: String,
        private val dataPipeline: DBToFilePipeline) : BasePageProcessor(tableName, context) {

    @Throws(SQLException::class, IOException::class)
    override fun execute(pageBlock: PageBlockData) {
        Thread.currentThread().name = "$tableName-${pageBlock.index}"

        val step = context.pageSize / 100.0

        var connection: Connection? = null
        try {
            val startPage = System.currentTimeMillis()
            LOGGER.debug("Starting {} page {}", tableName, pageBlock.index)

            val command = Callable{ context[PoolType.SOURCE] }
            val getConnection = RetryCall(command, 30, 2000, true)
            connection = getConnection.call()

            connection!!.createStatement().use { statement ->
                statement.fetchSize = min(FETCH_SIZE.toLong(), pageBlock.length).toInt()
                val sql = SQLStatementFactory.getPageSQL(connection, pageBlock, context.srcSchema, tableName)

                statement.executeQuery(sql).use { resultSet ->
                    val columnCount = getColumnCount(resultSet, sql)

                    var counter = 0
                    while (dataPipeline.next(resultSet, columnCount)) {
                        counter++

                        if (counter % step == 0.0) {
                            dataPipeline.flush()
                            val percent = (counter / pageBlock.length * 100.0).roundToLong()
                            LOGGER.debug("\t{}%", percent)
                        }
                    }
                }
            }

            dataPipeline.flush()

            if (LOGGER.isDebugEnabled) {
                val duration = format.format(System.currentTimeMillis() - startPage)
                LOGGER.debug("Finished {} page {} in {} ms", tableName, pageBlock.index, duration)
            }
        } finally {
            try {
                if (connection != null) {
                    context.release(PoolType.SOURCE, connection)
                }
            } catch (e: Exception) {
                LOGGER.error(e.message, e)
            }
        }
    }

    @Throws(SQLException::class)
    private fun getColumnCount(resultSet: ResultSet, sql: String): Int {
        val metaData = resultSet.metaData
        var columnCount = metaData.columnCount
        if (sql.contains("RNUM")) {// remove ROWNUM column
            columnCount--
        }
        return columnCount
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PageExportProcessor::class.java)
    }
}
