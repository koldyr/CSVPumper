package com.koldyr.csv.processor.copy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.Constants;
import com.koldyr.csv.db.SQLStatementFactory;
import com.koldyr.csv.io.DbToDbPipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.PoolType;
import com.koldyr.csv.model.ProcessorContext;
import com.koldyr.csv.processor.BatchDBProcessor;

/**
 * Description of class CopyProcessor
 *
 * @created: 2018.03.18
 */
public class CopyProcessor extends BatchDBProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CopyProcessor.class);

    public CopyProcessor(ProcessorContext context) {
        super(context);
    }

    @Override
    protected void processTable(String tableName) {
        long start = System.currentTimeMillis();
        Thread.currentThread().setName(tableName);

        DbToDbPipeline dataPipeline = new DbToDbPipeline();

        Connection srcConnection = null;
        Connection dstConnection = null;
        try {
            srcConnection = context.get(PoolType.SOURCE);
            dstConnection = context.get(PoolType.DESTINATION);
            long rowCount = getRowCount(srcConnection, tableName);

            LOGGER.debug("Starting table {}: {} rows", tableName, format.format(rowCount));

            if (rowCount > context.getPageSize()) {
                release(srcConnection, PoolType.SOURCE);
                release(dstConnection, PoolType.DESTINATION);
                srcConnection = null; //don't release this connection in finally block
                dstConnection = null; //don't release this connection in finally block

                parallelCopy(dataPipeline, tableName, rowCount);
            } else {
                copy(srcConnection, dstConnection, dataPipeline, tableName, rowCount);
            }

            LOGGER.debug("Finished table {}: {} rows in {} ms", tableName, format.format(rowCount), format.format(System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            release(srcConnection, PoolType.SOURCE);
            release(dstConnection, PoolType.DESTINATION);
        }
    }

    private void parallelCopy(DbToDbPipeline dataPipeline, String tableName, long rowCount) throws Exception {
        int pageCount = (int) Math.ceil(rowCount / (double) context.getPageSize());

        LOGGER.debug("Copy {} pages", pageCount);

        List<PageBlockData> pages = new ArrayList<>(pageCount);

        for (int i = 0; i < pageCount; i++) {
            long start = i * context.getPageSize();
            long length = Math.min(context.getPageSize(), rowCount - start);
            pages.add(new PageBlockData(i, start, length));
        }

        context.setPages(tableName, pages);

        int columnCount;
        Connection connection = null;
        try {
            connection = context.get(PoolType.SOURCE);
            columnCount = getColumnCount(connection, tableName);
        } finally {
            context.release(PoolType.SOURCE, connection);
        }

        final String sqlInsert = SQLStatementFactory.getInsertValues(connection, context.getDstSchema(), tableName, columnCount);

        int threadCount = Math.min(Constants.PARALLEL_PAGES, pageCount);
        final Collection<Callable<Integer>> copyThreads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            copyThreads.add(new PageCopyProcessor(context, tableName, dataPipeline, sqlInsert));
        }

        final List<Future<Integer>> results = context.getExecutor().invokeAll(copyThreads);

        checkResults(tableName, pageCount, results);
    }

    private void copy(Connection srcConnection, Connection dstConnection, DbToDbPipeline dataPipeline, String tableName, long rowCount) throws SQLException {
        final double step = context.getPageSize() / 100.0;

        final int columnCount = getColumnCount(srcConnection, tableName);
        final String insertSql = SQLStatementFactory.getInsertValues(dstConnection, context.getDstSchema(), tableName, columnCount);
        ResultSet resultSet = null;
        try (Statement srcStatement = srcConnection.createStatement();
             PreparedStatement dstStatement = dstConnection.prepareStatement(insertSql)) {
            final String selectAll = SQLStatementFactory.getSelectAll(srcConnection, context.getSrcSchema(), tableName);
            resultSet = srcStatement.executeQuery(selectAll);

            int counter = 0;
            while (dataPipeline.next(resultSet, dstStatement)) {
                counter++;

                if (counter % step == 0) {
                    dstStatement.executeBatch();
                    dstConnection.commit();
                    final long percent = Math.round(counter / rowCount * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }
            dstStatement.executeBatch();
            dstConnection.commit();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    private int getColumnCount(Connection connection, String tableName) throws SQLException {
        ResultSet resultSet = null;
        try (Statement statement = connection.createStatement()) {
            final String selectAll = SQLStatementFactory.getSelectAll(connection, context.getSrcSchema(), tableName);
            resultSet = statement.executeQuery(selectAll);
            return resultSet.getMetaData().getColumnCount();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }
}
