package com.koldyr.csv.processor;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.io.DataPipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.ProcessorContext;

/**
 * Description of class PageExportProcessor
 *
 * @created: 2018.03.05
 */
class PageExportProcessor implements Callable<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PageExportProcessor.class);

    private final ProcessorContext context;
    private final String tableName;
    private final DataPipeline dataPipeline;
    private final DecimalFormat format;

    public PageExportProcessor(ProcessorContext context, String tableName, DataPipeline dataPipeline) {
        this.context = context;
        this.tableName = tableName;
        this.dataPipeline = dataPipeline;

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

        Statement statement = null;
        ResultSet resultSet = null;

        try {
            long startPage = System.currentTimeMillis();
            LOGGER.debug("Starting {} page  {}", tableName, pageBlock.index);

            final CallWithRetry<Statement> getStatement = new CallWithRetry<>(() -> context.getConnection().createStatement(),
                    10, 1000, true);
            statement = getStatement.call();

            String sql = "SELECT * FROM (SELECT subQ.*, rownum RNUM FROM ( SELECT * FROM " + context.getSchema() + '.' + tableName +
                    ") subQ WHERE rownum <= " + (pageBlock.start + pageBlock.length) + ") WHERE RNUM > " + pageBlock.start;

            resultSet = statement.executeQuery(sql);

            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();

            int counter = 0;
            while (resultSet.next()) {
                dataPipeline.writeRow(resultSet, columnCount);

                counter++;

                if (counter % 1000.0 == 0) {
                    dataPipeline.flush();
                    final long percent = Math.round(counter / (double) pageBlock.length * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }

            dataPipeline.flush();

            LOGGER.debug("Finished {} page {} in {} msec", tableName, pageBlock.index, format.format(System.currentTimeMillis() - startPage));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
