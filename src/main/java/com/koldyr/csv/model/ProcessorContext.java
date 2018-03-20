package com.koldyr.csv.model;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.KeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description of class ProcessorConfig
 *
 * @created: 2018.03.04
 */
public class ProcessorContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorContext.class);

    private final ReadWriteLock queueLock = new ReentrantReadWriteLock();
    private final ReadWriteLock pagesLock = new ReentrantReadWriteLock();

    private final KeyedObjectPool<PoolType, Connection> connectionsPool;
    private final List<String> tableNames;
    private String srcSchema;
    private String dstSchema;
    private String path;

    private ExecutorService executor;
    private long pageSize;
    private Map<String, List<PageBlockData>> pages = new ConcurrentHashMap<>(0);

    public ProcessorContext(KeyedObjectPool<PoolType, Connection> connectionsPool, List<String> tableNames) {
        this.connectionsPool = connectionsPool;
        this.tableNames = tableNames;
    }

    public String getSrcSchema() {
        return srcSchema;
    }

    public void setSrcSchema(String srcSchema) {
        this.srcSchema = srcSchema;
    }

    public String getDstSchema() {
        return dstSchema;
    }

    public void setDstSchema(String dstSchema) {
        this.dstSchema = dstSchema;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public long getPageSize() {
        return pageSize;
    }

    public void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }

    public void setPages(String tableName, List<PageBlockData> pages) {
        this.pages.put(tableName, pages);
    }

    public PageBlockData getNextPageBlock(String tableName) {
        final Lock lock = pagesLock.readLock();
        lock.lock();
        try {
            final List<PageBlockData> tablePages = pages.get(tableName);
            return tablePages.size() > 0 ? tablePages.remove(0) : null;
        } finally {
            lock.unlock();
        }
    }

    public String getNextTable() {
        final Lock lock = queueLock.readLock();
        lock.lock();
        try {
            return tableNames.size() > 0 ? tableNames.remove(0) : null;
        } finally {
            lock.unlock();
        }
    }

    public Connection get(PoolType type) throws Exception {
        final Connection connection = connectionsPool.borrowObject(type);
        LOGGER.debug("Active {} connections: {}", type, connectionsPool.getNumActive(type));
        return connection;
    }

    public void release(PoolType type, Connection connection) throws Exception {
        connectionsPool.returnObject(type, connection);
        LOGGER.debug("Active {} connections: {}", type, connectionsPool.getNumActive(type));
    }
}
