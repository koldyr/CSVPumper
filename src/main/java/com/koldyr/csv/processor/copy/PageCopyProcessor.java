package com.koldyr.csv.processor.copy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.io.DbToDbPipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.PoolType;
import com.koldyr.csv.model.ProcessorContext;
import com.koldyr.csv.processor.BasePageProcessor;
import com.koldyr.csv.processor.CallWithRetry;

/**
 * Description of class PageCopyProcessor
 *
 * @created: 2018.03.18
 */
public class PageCopyProcessor extends BasePageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PageCopyProcessor.class);

    private final DbToDbPipeline dataPipeline;
    private final String sqlInsert;

    public PageCopyProcessor(ProcessorContext context, String tableName, DbToDbPipeline dataPipeline, String sqlInsert) {
        super(tableName, context);
        this.dataPipeline = dataPipeline;
        this.sqlInsert = sqlInsert;
    }

    @Override
    protected void execute(PageBlockData pageBlock) throws SQLException {
        Thread.currentThread().setName(tableName + '-' + pageBlock.index);

        final double step = context.getPageSize() / 100.0;

        Connection srcConnection = null;
        Statement srcStatement = null;
        ResultSet srcResultSet = null;

        Connection dstConnection = null;
        PreparedStatement dstStatement = null;

        try {
            long startPage = System.currentTimeMillis();
            LOGGER.debug("Starting {} page {}", tableName, pageBlock.index);

            final CallWithRetry<Connection> getSrcConnection = new CallWithRetry<>(() -> context.get(PoolType.SOURCE), 30, 2000, true);
            srcConnection = getSrcConnection.call();
            srcStatement = srcConnection.createStatement();

            String sqlGetPage = getPageSQL(srcConnection, pageBlock);

            srcResultSet = srcStatement.executeQuery(sqlGetPage);

            final CallWithRetry<Connection> getDstConnection = new CallWithRetry<>(() -> context.get(PoolType.DESTINATION), 30, 2000, true);
            dstConnection = getDstConnection.call();
            dstStatement = dstConnection.prepareStatement(sqlInsert);

            int counter = 0;
            while (dataPipeline.next(srcResultSet, dstStatement)) {
                counter++;

                if (counter % step == 0) {
                    dstStatement.executeBatch();
                    final long percent = Math.round(counter / (double) pageBlock.length * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }

            dstStatement.executeBatch();

            LOGGER.debug("Finished {} page {} in {} ms", tableName, pageBlock.index, format.format(System.currentTimeMillis() - startPage));
        } finally {
            try {
                if (srcResultSet != null) {
                    srcResultSet.close();
                }
                if (srcStatement != null) {
                    srcStatement.close();
                }
                if (srcConnection != null) {
                    context.release(PoolType.SOURCE, srcConnection);
                }

                if (dstStatement != null) {
                    dstStatement.close();
                }
                if (dstConnection != null) {
                    context.release(PoolType.DESTINATION, dstConnection);
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
