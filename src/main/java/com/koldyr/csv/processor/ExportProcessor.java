package com.koldyr.csv.processor;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.io.DataPipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.ProcessorContext;

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

        Statement statement = null;
        ResultSet resultSet = null;
        try (FileOutputStream outputStream = new FileOutputStream(context.getPath() + '/' + tableName + ".csv")) {
            final Connection connection = context.getConnection();
            long rowCount = getRowCount(connection, tableName);
            LOGGER.debug("Starting {}: {}", tableName, format.format(rowCount));

            final DataPipeline dataPipeline = new DataPipeline(outputStream);

            if (rowCount > context.getPageSize()) {
                parallelExport(tableName, dataPipeline, rowCount);
            } else {
                statement = connection.createStatement();
                resultSet = statement.executeQuery("SELECT * FROM " + context.getSchema() + '.' + tableName);

                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();

                int counter = 0;
                while (resultSet.next()) {
                    dataPipeline.writeRow(resultSet, columnCount);

                    counter++;

                    if (counter % 1000.0 == 0) {
                        dataPipeline.flush();
                        final long percent = Math.round(counter / (double) rowCount * 100.0);
                        LOGGER.debug("\t{}%", percent);
                    }
                }
            }

            dataPipeline.flush();
            dataPipeline.close();

            LOGGER.debug("Finished {}: {} rows in {} msec", tableName, format.format(rowCount), format.format(System.currentTimeMillis() - start));
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

    private void parallelExport(String tableName, DataPipeline dataPipeline, long rowCount) throws InterruptedException {
        int pageCount = (int) Math.ceil(rowCount / (double) context.getPageSize());

        LOGGER.debug("Pages: {}", pageCount);

        List<PageBlockData> pages = new ArrayList<>(pageCount);

        for (int i = 0; i < pageCount; i++) {
            long start = i * context.getPageSize();
            long length = Math.min(context.getPageSize(), rowCount - start);
            pages.add(new PageBlockData(i, start, length));
        }

        context.setPages(tableName, pages);

        int threadCount = Math.min(48, pageCount);
        final Collection<Callable<Object>> exportThreads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            exportThreads.add(new PageExportProcessor(context, tableName, dataPipeline));
        }

        context.getExecutor().invokeAll(exportThreads);
    }

    private long getRowCount(Connection connection, String tableName) throws SQLException {
        long rowCount = 0;

        ResultSet resultSet = null;
        try (Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery("SELECT count(*) FROM " + context.getSchema() + '.' + tableName);

            if (resultSet.next()) {
                rowCount = resultSet.getLong(1);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return rowCount;
    }
}
