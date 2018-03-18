/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.processor.copy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.io.DbToDbPipeline;
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

    private void parallelCopy(DbToDbPipeline dataPipeline, String tableName, long rowCount) {

    }

    private void copy(Connection srcConnection, Connection dstConnection, DbToDbPipeline dataPipeline, String tableName, long rowCount) throws SQLException {
        final double step = context.getPageSize() / 100.0;

        int columnCount = getColumnCount(srcConnection, tableName);
        String insertSql = createInsertSql(tableName, columnCount);
        ResultSet resultSet = null;
        try (Statement srcStatement = srcConnection.createStatement();
             PreparedStatement dstStatement = dstConnection.prepareStatement(insertSql)) {
            resultSet = srcStatement.executeQuery("SELECT * FROM " + context.getSchema() + '.' + tableName);

            int counter = 0;
            while (dataPipeline.next(resultSet, dstStatement)) {
                counter++;

                if (counter % step == 0) {
                    dstStatement.executeBatch();
                    final long percent = Math.round(counter / rowCount * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }
            dstStatement.executeBatch();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    private int getColumnCount(Connection connection, String tableName) throws SQLException {
        ResultSet resultSet = null;
        try (Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery("SELECT * FROM \"" + context.getSchema() + "\".\"" + tableName + '"');
            return resultSet.getMetaData().getColumnCount();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }
}
