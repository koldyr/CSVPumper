package com.koldyr.csv.processor.imprt

import com.koldyr.csv.Constants
import com.koldyr.csv.db.DatabaseDetector.isOracle
import com.koldyr.csv.db.SQLStatementFactory
import com.koldyr.csv.io.FileToDBPipeline
import com.koldyr.csv.model.PageBlockData
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import com.koldyr.csv.processor.BatchDBProcessor
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.charset.StandardCharsets.*
import java.nio.file.Files.*
import java.nio.file.Path
import java.sql.Connection
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.concurrent.Callable
import kotlin.math.ceil
import kotlin.math.min
import kotlin.math.roundToLong

/**
 * Description of class ImportProcessor
 *
 * @created: 2018.03.03
 */
class ImportProcessor(context: ProcessorContext) : BatchDBProcessor(context) {

    override fun processTable(tableName: String) {
        val start = System.currentTimeMillis()

        Thread.currentThread().name = tableName
        LOGGER.debug("Starting table {}", tableName)

        var connection: Connection? = null

        val csvFile = Path.of(context.path, "$tableName.csv")
        try {
            FileToDBPipeline(csvFile).use { pipeline ->
                connection = context[PoolType.DESTINATION]

                val usable = connection!!
                if (!isOracle(usable)) {
                    usable.schema = context.dstSchema
                }

                val rowCount = getRowCount(csvFile)

                if (rowCount > context.pageSize) {
                    parallelImport(usable, pipeline, tableName, rowCount)
                } else {
                    singleImport(usable, pipeline, tableName, rowCount)
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
            release(connection, PoolType.DESTINATION)
        }
    }

    @Throws(IOException::class)
    private fun getRowCount(csvFile: Path): Long {
        return newBufferedReader(csvFile, UTF_8).use { reader ->
            reader.lines().count()
        }
    }

    @Throws(InterruptedException::class, SQLException::class)
    private fun parallelImport(connection: Connection, pipeline: FileToDBPipeline, tableName: String, rowCount: Long) {
        val pageCount = ceil(rowCount / context.pageSize.toDouble()).toInt()

        LOGGER.debug("Import {} pages", pageCount)

        val pages = ArrayList<PageBlockData>(pageCount)

        val threadCount = min(Constants.PARALLEL_PAGES, pageCount)
        for (i in 0 until threadCount) {
            pages.add(PageBlockData(i, 0, rowCount))
        }

        context.setPages(tableName, pages)

        val metaData = getMetaData(connection, tableName)
        val insertSql = SQLStatementFactory.getInsertValues(connection, context.dstSchema, tableName, metaData.columnCount)

        val importThreads = ArrayList<Callable<Int>>(threadCount)
        for (i in 0 until threadCount) {
            importThreads.add(PageImportProcessor(context, tableName, metaData, pipeline, insertSql))
        }

        val results = context.executor.invokeAll(importThreads)

        checkResults(tableName, threadCount, results)
    }

    @Throws(SQLException::class, IOException::class)
    private fun singleImport(connection: Connection, dataPipeline: FileToDBPipeline, tableName: String, rowCount: Long) {
        val step = context.pageSize / 100.0

        val metaData = getMetaData(connection, tableName)
        val sql = SQLStatementFactory.getInsertValues(connection, context.dstSchema, tableName, metaData.columnCount)
        val statement = connection.prepareStatement(sql)

        while (dataPipeline.next(statement, metaData)) {
            if (dataPipeline.counter() % step == 0.0) {
                statement.executeBatch()
                connection.commit()
                dataPipeline.closeBatch()

                val percent = (dataPipeline.counter() / rowCount.toDouble() * 100.0).roundToLong()
                LOGGER.debug("\t{}%", percent)
            }
        }

        statement.executeBatch()
        connection.commit()
        dataPipeline.closeBatch()
    }

    /**
     * Intentionally left resources opened. Not all drivers allow work with ResultSetMetaData after statement closed.
     * @param connection
     * @param tableName
     * @return
     * @throws SQLException
     */
    @Throws(SQLException::class)
    private fun getMetaData(connection: Connection, tableName: String): ResultSetMetaData {
        val selectAll = SQLStatementFactory.getSelectAll(connection, context.dstSchema, tableName)
        val statement = connection.createStatement()
        statement.fetchSize = 1
        val resultSet = statement.executeQuery(selectAll)
        return resultSet.metaData
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ImportProcessor::class.java)
    }
}
