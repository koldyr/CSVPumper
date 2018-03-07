package com.koldyr.csv.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.sql.DataSource;

/**
 * Description of class ProcessorConfig
 *
 * @created: 2018.03.04
 */
public class ProcessorContext {

    private final ReadWriteLock queueLock = new ReentrantReadWriteLock();
    private final ReadWriteLock pagesLock = new ReentrantReadWriteLock();

    private final DataSource dataSource;
    private final List<String> tableNames;
    private String schema;
    private String path;

    private ExecutorService executor;
    private long pageSize;
    private Map<String, List<PageBlockData>> pages = new ConcurrentHashMap<>(0);

    public ProcessorContext(DataSource dataSource, List<String> tableNames) {
        this.dataSource = dataSource;
        this.tableNames = tableNames;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
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

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
