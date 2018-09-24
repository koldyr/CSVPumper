package com.koldyr.csv.processor.export;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import com.koldyr.csv.io.DBToFilePipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.PoolType;
import com.koldyr.csv.model.ProcessorContext;
import com.koldyr.csv.processor.BatchDBProcessor;

/**
 * Description of class ExportProcessor
 *
 * @created: 2018.03.03
 */
public class ExportProcessor extends BatchDBProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportProcessor.class);

    public ExportProcessor(ProcessorContext config) {
        super(config);
    }

    @Override
    protected void processTable(String tableName) {
        long start = System.currentTimeMillis();
        Thread.currentThread().setName(tableName);

        final String fileName = context.getPath() + '/' + tableName + ".csv";

        Connection connection = null;
        try (DBToFilePipeline dataPipeline = new DBToFilePipeline(fileName)) {
            connection = context.get(PoolType.SOURCE);
            long rowCount = getRowCount(connection, tableName);

            LOGGER.debug("Starting table {}: {} rows", tableName, format.format(rowCount));

            if (rowCount > context.getPageSize()) {
                release(connection, PoolType.SOURCE);
                connection = null; //don't release this connection in finally block

                parallelExport(dataPipeline, tableName, rowCount);
            } else {
                export(connection, dataPipeline, tableName, rowCount);
            }

            LOGGER.debug("Finished table {}: {} rows in {} ms", tableName, format.format(rowCount), format.format(System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            release(connection, PoolType.SOURCE);
        }
    }

    private void export(Connection connection, DBToFilePipeline dataPipeline, String tableName, double rowCount) throws SQLException, IOException {
        final double step = context.getPageSize() / 100.0;

        ResultSet resultSet = null;
        try (Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery("SELECT * FROM \"" + context.getSrcSchema() + "\".\"" + tableName + "\"");

            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();

            int counter = 0;
            while (dataPipeline.next(resultSet, columnCount)) {
                counter++;

                if (counter % step == 0) {
                    dataPipeline.flush();
                    final long percent = Math.round(counter / rowCount * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    private void parallelExport(DBToFilePipeline dataPipeline, String tableName, long rowCount) throws InterruptedException {
        int pageCount = (int) Math.ceil(rowCount / (double) context.getPageSize());

        LOGGER.debug("Export {} pages", pageCount);

        List<PageBlockData> pages = new ArrayList<>(pageCount);

        for (int i = 0; i < pageCount; i++) {
            long start = i * context.getPageSize();
            long length = Math.min(context.getPageSize(), rowCount - start);
            pages.add(new PageBlockData(i, start, length));
        }

        context.setPages(tableName, pages);

        int threadCount = Math.min(Constants.PARALLEL_PAGES, pageCount);
        final Collection<Callable<Integer>> exportThreads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            exportThreads.add(new PageExportProcessor(context, tableName, dataPipeline));
        }

        final List<Future<Integer>> results = context.getExecutor().invokeAll(exportThreads);

        checkResults(tableName, pageCount, results);
    }
}
