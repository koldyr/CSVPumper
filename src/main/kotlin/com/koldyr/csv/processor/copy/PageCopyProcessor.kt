package com.koldyr.csv.processor.copy

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.Callable
import kotlin.math.min
import kotlin.math.roundToLong
import org.slf4j.LoggerFactory
import com.koldyr.csv.Constants.FETCH_SIZE
import com.koldyr.csv.db.SQLStatementFactory
import com.koldyr.csv.io.DbToDbPipeline
import com.koldyr.csv.model.PageBlockData
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import com.koldyr.csv.processor.BasePageProcessor
import com.koldyr.csv.processor.RetryCall

/**
 * Description of class PageCopyProcessor
 *
 * @created: 2018.03.18
 */
class PageCopyProcessor(
    context: ProcessorContext,
    tableName: String,
    private val dataPipeline: DbToDbPipeline,
    private val sqlInsert: String
) : BasePageProcessor(tableName, context) {

    @Throws(SQLException::class)
    override fun execute(pageBlock: PageBlockData) {
        Thread.currentThread().name = "$tableName-${pageBlock.index}"

        val step = context.pageSize / 100.0

        var srcConnection: Connection? = null
        var srcStatement: Statement? = null
        var srcResultSet: ResultSet? = null

        var dstConnection: Connection? = null
        var dstStatement: PreparedStatement? = null

        try {
            val startPage = System.currentTimeMillis()
            LOGGER.debug("Starting {} page {}", tableName, pageBlock.index)

            val commandSrcConn = Callable { context[PoolType.SOURCE] }
            val getSrcConnection = RetryCall(commandSrcConn, 30, 2000, true)
            srcConnection = getSrcConnection.call()
            srcStatement = srcConnection!!.createStatement()
            srcStatement!!.fetchSize = min(FETCH_SIZE.toLong(), pageBlock.length).toInt()

            val sql = SQLStatementFactory.getPageSQL(srcConnection, pageBlock, context.srcSchema, tableName)
            srcResultSet = srcStatement.executeQuery(sql)

            val commandDstConn = Callable { context[PoolType.DESTINATION] }
            val getDstConnection = RetryCall(commandDstConn, 30, 2000, true)
            dstConnection = getDstConnection.call()
            dstStatement = dstConnection!!.prepareStatement(sqlInsert)

            var counter = 0
            while (dataPipeline.next(srcResultSet!!, dstStatement)) {
                counter++

                if (counter % step == 0.0) {
                    dstStatement!!.executeBatch()
                    dstConnection.commit()
                    val percent = (counter / pageBlock.length * 100.0).roundToLong()
                    LOGGER.debug("\t{}%", percent)
                }
            }

            dstStatement!!.executeBatch()
            dstConnection.commit()

            if (LOGGER.isDebugEnabled) {
                val duration = format.format(System.currentTimeMillis() - startPage)
                LOGGER.debug("Finished {} page {} in {} ms", tableName, pageBlock.index, duration)
            }
        } finally {
            try {
                srcResultSet?.close()
                srcStatement?.close()
                srcConnection?.let {
                    context.release(PoolType.SOURCE, srcConnection)
                }

                dstStatement?.close()
                dstConnection?.let {
                    context.release(PoolType.DESTINATION, dstConnection)
                }
            } catch (e: Exception) {
                LOGGER.error(e.message, e)
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PageCopyProcessor::class.java)
    }
}
