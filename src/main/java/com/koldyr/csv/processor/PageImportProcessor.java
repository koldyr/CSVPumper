/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.concurrent.Callable;

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
public class PageImportProcessor implements Callable<Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PageImportProcessor.class);

    private final ProcessorContext context;
    private final String tableName;
    private final ResultSetMetaData metaData;
    private final FileToDBPipeline dataPipeline;
    private final String insertSql;
    private final DecimalFormat format;

    public PageImportProcessor(ProcessorContext context, String tableName, ResultSetMetaData metaData, FileToDBPipeline dataPipeline, String insertSql) {
        this.context = context;
        this.tableName = tableName;
        this.metaData = metaData;

        this.dataPipeline = dataPipeline;
        this.insertSql = insertSql;

        final DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setGroupingSeparator(',');
        format = new DecimalFormat("###,###,###,###", decimalFormatSymbols);
    }

    @Override
    public Object call() {
        PageBlockData pageBlock = context.getNextPageBlock(tableName);
        while (pageBlock != null) {
            execute(pageBlock);
            pageBlock = context.getNextPageBlock(tableName);
        }

        return null;
    }

    private void execute(PageBlockData pageBlock) {
        Thread.currentThread().setName(tableName + '-' + pageBlock.index);

        Connection connection = null;
        PreparedStatement statement = null;

        try {
            long startPage = System.currentTimeMillis();
            LOGGER.debug("Starting {} page  {}", tableName, pageBlock.index);

            final CallWithRetry<Connection> getConnection = new CallWithRetry<>(context::get, 10, 1000, true);
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
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
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
