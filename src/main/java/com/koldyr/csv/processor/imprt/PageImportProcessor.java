package com.koldyr.csv.processor.imprt;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.io.FileToDBPipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.PoolType;
import com.koldyr.csv.model.ProcessorContext;
import com.koldyr.csv.processor.BasePageProcessor;
import com.koldyr.csv.processor.RetryCall;

/**
 * Description of class PageImportProcessor
 *
 * @created: 2018.03.07
 */
public class PageImportProcessor extends BasePageProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PageImportProcessor.class);

    private final ResultSetMetaData metaData;
    private final FileToDBPipeline dataPipeline;
    private final String insertSql;

    public PageImportProcessor(ProcessorContext context, String tableName, ResultSetMetaData metaData, FileToDBPipeline dataPipeline, String insertSql) {
        super(tableName, context);
        this.metaData = metaData;

        this.dataPipeline = dataPipeline;
        this.insertSql = insertSql;
    }

    @Override @SuppressWarnings("resource")
    protected void execute(PageBlockData pageBlock) throws SQLException, IOException {
        Thread.currentThread().setName(tableName + '-' + pageBlock.index);

        final double step = context.getPageSize() / 100.0;
        final double totalRowCount = pageBlock.length;

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            long startPage = System.currentTimeMillis();
            LOGGER.debug("Starting {} page {}", tableName, pageBlock.index);

            final RetryCall<Connection> getConnection = new RetryCall<>(() -> context.get(PoolType.DESTINATION), 30, 1000, true);
            connection = getConnection.call();

            statement = connection.prepareStatement(insertSql);

            int counter = 0;
            while (dataPipeline.next(statement, metaData)) {
                counter++;

                if (counter % step == 0) {
                    final RetryCall<int[]> executeBatch = new RetryCall<>(statement::executeBatch, 3, 1000, false);
                    executeBatch.call();
                    connection.commit();
                    dataPipeline.closeBatch();

                    final long percent = Math.round(dataPipeline.counter() / totalRowCount * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }

            statement.executeBatch();
            connection.commit();
            dataPipeline.closeBatch();

            if (LOGGER.isDebugEnabled()) {
                String duration = format.format(System.currentTimeMillis() - startPage);
                LOGGER.debug("Finished {} page {} in {} ms", tableName, pageBlock.index, duration);
            }
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }

                if (connection != null) {
                    context.release(PoolType.DESTINATION, connection);
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
