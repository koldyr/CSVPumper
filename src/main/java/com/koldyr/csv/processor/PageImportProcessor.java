/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.processor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.io.FileToDBPipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.ProcessorContext;

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

    @Override
    protected void execute(PageBlockData pageBlock) throws SQLException, IOException {
        Thread.currentThread().setName(tableName + '-' + pageBlock.index);

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            long startPage = System.currentTimeMillis();
            LOGGER.debug("Starting {} page  {}", tableName, pageBlock.index);

            final CallWithRetry<Connection> getConnection = new CallWithRetry<>(context::get, 30, 1000, true);
            connection = getConnection.call();

            statement = connection.prepareStatement(insertSql);

            int counter = 0;
            while (dataPipeline.next(statement, metaData)) {
                counter++;

                if (counter % 100.0 == 0) {
                    statement.executeBatch();
                    final long percent = Math.round(dataPipeline.counter() / (double) pageBlock.length * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }

            statement.executeBatch();

            LOGGER.debug("Finished {} page {} in {} ms", tableName, pageBlock.index, format.format(System.currentTimeMillis() - startPage));
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }

                if (connection != null) {
                    context.release(connection);
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
