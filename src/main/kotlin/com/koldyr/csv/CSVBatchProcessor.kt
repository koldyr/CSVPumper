package com.koldyr.csv

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.charset.StandardCharsets.*
import java.nio.file.Files
import java.sql.Connection
import java.time.Duration.*
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.math.min
import org.apache.commons.pool2.KeyedObjectPool
import org.apache.commons.pool2.impl.GenericKeyedObjectPool
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig
import org.slf4j.LoggerFactory
import com.koldyr.csv.db.ConnectionsFactory
import com.koldyr.csv.model.ConnectionData
import com.koldyr.csv.model.Operation
import com.koldyr.csv.model.PoolType
import com.koldyr.csv.model.ProcessorContext
import com.koldyr.csv.processor.copy.CopyProcessor
import com.koldyr.csv.processor.export.ExportProcessor
import com.koldyr.csv.processor.imprt.ImportProcessor

/**
 * Description of class CSVExport
 *
 * @created: 2018.03.02
 */
object CSVBatchProcessor {

    private val LOGGER = LoggerFactory.getLogger(CSVBatchProcessor::class.java)

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val dbConfig = Properties()

        FileInputStream("db-config.properties").use { input -> dbConfig.load(input) }

        val operation = Operation.valueOf(dbConfig.getProperty("operation"))
        val srcConfig = getDBConfig(dbConfig, "source")
        val dstConfig = getDBConfig(dbConfig, "destination")
        val path = dbConfig.getProperty("path")

        val connectionsPool = createConnectionsPool(srcConfig, dstConfig)

        LOGGER.debug("Load table names...")
        val tableNames = loadTableNames()

        loadProcessConfig(connectionsPool)

        val threadCount = min(Constants.PARALLEL_TABLES, tableNames.size)
        val executor = Executors.newCachedThreadPool()

        try {
            LOGGER.debug("Create processors")

            val context = ProcessorContext(connectionsPool, tableNames, executor, dstConfig.schema, srcConfig.schema)
            context.path = path

            val processors = LinkedList<Callable<Any>>()
            for (i in 0 until threadCount) {
                processors.add(createProcessor(operation, context))
            }

            LOGGER.debug("Start...")
            executor.invokeAll(processors)
        } catch (e: InterruptedException) {
            LOGGER.error(e.message, e)
        } finally {
            connectionsPool.close()
            executor.shutdown()
        }
    }

    private fun getDBConfig(params: Properties, prefix: String): ConnectionData {
        return ConnectionData(params.getProperty("$prefix.url"),
            params.getProperty("$prefix.schema"),
            params.getProperty("$prefix.user"),
            params.getProperty("$prefix.password"))
    }

    private fun loadProcessConfig(connectionsPool: KeyedObjectPool<PoolType, Connection>) {
        try {
            val config = Properties()
            config.load(FileInputStream("process-config.properties"))
            Constants.MAX_CONNECTIONS = Integer.parseInt(config.getProperty("max-connections"))
            Constants.PAGE_SIZE = java.lang.Long.parseLong(config.getProperty("page-size"))
            Constants.PARALLEL_TABLES = Integer.parseInt(config.getProperty("parallel-tables"))
            Constants.PARALLEL_PAGES = Integer.parseInt(config.getProperty("parallel-pages"))

            val srcMaxConnections = getMaxConnections(connectionsPool, PoolType.SOURCE)
            val dstMaxConnections = getMaxConnections(connectionsPool, PoolType.DESTINATION)

            val mxCons = listOf(Constants.MAX_CONNECTIONS, srcMaxConnections, dstMaxConnections)
            Constants.MAX_CONNECTIONS = Collections.min(mxCons)
        } catch (e: Exception) {
            LOGGER.error(e.message, e)
        }
    }

    @Throws(Exception::class)
    private fun getMaxConnections(connectionsPool: KeyedObjectPool<PoolType, Connection>, type: PoolType): Int {
        val result = connectionsPool.borrowObject(type).use { connection ->
            try {
                val databaseMetaData = connection!!.metaData
                if (databaseMetaData.maxConnections == 0) {
                    Integer.MAX_VALUE
                }
                databaseMetaData.maxConnections
            } catch (e: Exception) {
                Integer.MAX_VALUE
            } finally {
                connectionsPool.returnObject(type, connection)
            }
        }
        return result
    }

    private fun createConnectionsPool(srcConfig: ConnectionData, dstConfig: ConnectionData): KeyedObjectPool<PoolType, Connection> {
        val factory = ConnectionsFactory(srcConfig, dstConfig)
        val config = GenericKeyedObjectPoolConfig<Connection>()
        config.maxTotalPerKey = Constants.MAX_CONNECTIONS
        config.maxIdlePerKey = 1
        config.minEvictableIdleTime = ofMillis(100)
        config.numTestsPerEvictionRun = 100
        config.timeBetweenEvictionRuns = ofMillis(1000)
        return GenericKeyedObjectPool(factory, config)
    }

    private fun createProcessor(operation: Operation, context: ProcessorContext): Callable<Any> {
        return when (operation) {
            Operation.EXPORT -> ExportProcessor(context)
            Operation.IMPORT -> ImportProcessor(context)
            Operation.COPY -> CopyProcessor(context)
        }
    }

    private fun loadTableNames(): MutableList<String> {
        val tables = File("tables.config")
        if (!tables.exists()) {
            LOGGER.warn("No tables.config file found")
            try {
                tables.createNewFile()
            } catch (e: IOException) {
                LOGGER.error(e.message, e)
            }

            return mutableListOf()
        }

        try {
            val stream = Files.newInputStream(tables.toPath())
            return stream.bufferedReader(UTF_8).use {
                val result = LinkedList<String>()
                it.lines().forEach { line ->
                    if (!line.startsWith("#")) {
                        result.add(line)
                    }
                }
                result
            }
        } catch (e: IOException) {
            LOGGER.error(e.message, e)
        }

        return mutableListOf()
    }
}
