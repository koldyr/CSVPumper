package com.koldyr.csv.processor.imprt

import java.io.IOException
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.concurrent.Callable
import kotlin.math.roundToLong
import org.slf4j.LoggerFactory
import com.koldyr.csv.io.FileToDBPipeline
import com.koldyr.csv.model.PageBlockData
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import com.koldyr.csv.processor.BasePageProcessor
import com.koldyr.csv.processor.RetryCall

/**
 * Description of class PageImportProcessor
 *
 * @created: 2018.03.07
 */
class PageImportProcessor(
    context: ProcessorContext,
    tableName: String,
    private val metaData: ResultSetMetaData,
    private val pipeline: FileToDBPipeline,
    private val insertSql: String
) : BasePageProcessor(tableName, context) {

    @Throws(SQLException::class, IOException::class)
    override fun execute(pageBlock: PageBlockData) {
        Thread.currentThread().name = "$tableName-${pageBlock.index}"

        val step = context.pageSize / 100.0
        val totalRowCount = pageBlock.length.toDouble()

        var connection: Connection? = null
        var statement: PreparedStatement? = null

        try {
            val startPage = System.currentTimeMillis()
            LOGGER.debug("Starting {} page {}", tableName, pageBlock.index)

            val commandGetConnection: Callable<Connection> = Callable { context.get(PoolType.DESTINATION) }
            val getConnection = RetryCall(commandGetConnection, 30, 1000, true)
            connection = getConnection.call()

            statement = connection!!.prepareStatement(insertSql)

            var counter = 0
            while (pipeline.next(statement, metaData)) {
                counter++

                if (counter % step == 0.0) {
                    val commandExecuteBatch = Callable { statement!!.executeBatch() }
                    val executeBatch = RetryCall(commandExecuteBatch, 3, 1000, false)
                    executeBatch.call()
                    connection.commit()
                    pipeline.closeBatch()

                    val percent = (pipeline.counter() / totalRowCount * 100.0).roundToLong()
                    LOGGER.debug("\t{}%", percent)
                }
            }

            statement!!.executeBatch()
            connection.commit()
            pipeline.closeBatch()

            if (LOGGER.isDebugEnabled) {
                val duration = format.format(System.currentTimeMillis() - startPage)
                LOGGER.debug("Finished {} page {} in {} ms", tableName, pageBlock.index, duration)
            }
        } finally {
            try {
                statement?.close()

                connection?.let {
                    context.release(PoolType.DESTINATION, connection)
                }
            } catch (e: Exception) {
                LOGGER.error(e.message, e)
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PageImportProcessor::class.java)
    }
}
