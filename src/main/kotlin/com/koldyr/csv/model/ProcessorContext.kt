package com.koldyr.csv.model

import com.koldyr.csv.Constants
import org.apache.commons.pool2.KeyedObjectPool
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Description of class ProcessorConfig
 *
 * @created: 2018.03.04
 */
class ProcessorContext(
        private val connectionsPool: KeyedObjectPool<PoolType, Connection>,
        private val tableNames: MutableList<String>,
        val executor: ExecutorService,
        var dstSchema: String,
        var srcSchema: String
) {

    private val queueLock = ReentrantReadWriteLock()
    private val pagesLock = ReentrantReadWriteLock()
    var path: String? = null

    var pageSize: Long = Constants.PAGE_SIZE
    private val pages = ConcurrentHashMap<String, MutableList<PageBlockData>>(0)

    val nextTable: String?
        get() {
            val lock = queueLock.readLock()
            lock.lock()
            try {
                return if (tableNames.size > 0) tableNames.removeAt(0) else null
            } finally {
                lock.unlock()
            }
        }

    fun setPages(tableName: String, pages: MutableList<PageBlockData>) {
        this.pages[tableName] = pages
    }

    fun getNextPageBlock(tableName: String): PageBlockData? {
        val lock = pagesLock.readLock()
        lock.lock()
        try {
            val tablePages: MutableList<PageBlockData> = pages[tableName]!!
            return if (tablePages.size > 0) tablePages.removeAt(0) else null
        } finally {
            lock.unlock()
        }
    }

    @Throws(Exception::class)
    operator fun get(type: PoolType): Connection {
        val connection = connectionsPool.borrowObject(type)
        LOGGER.debug("Active {} connections: {}", type, connectionsPool.getNumActive(type))
        return connection
    }

    @Throws(Exception::class)
    fun release(type: PoolType, connection: Connection?) {
        connectionsPool.returnObject(type, connection)
        LOGGER.debug("Active {} connections: {}", type, connectionsPool.getNumActive(type))
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ProcessorContext::class.java)
    }
}
