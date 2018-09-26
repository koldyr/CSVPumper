package com.koldyr.csv.processor.imprt;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.koldyr.csv.db.DatabaseDetector.isOracle;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.koldyr.csv.Constants;
import com.koldyr.csv.db.SQLStatementFactory;
import com.koldyr.csv.io.FileToDBPipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.PoolType;
import com.koldyr.csv.model.ProcessorContext;
import com.koldyr.csv.processor.BatchDBProcessor;

/**
 * Description of class ImportProcessor
 *
 * @created: 2018.03.03
 */
public class ImportProcessor extends BatchDBProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportProcessor.class);

    public ImportProcessor(ProcessorContext config) {
        super(config);
    }

    @Override
    protected void processTable(String tableName) {
        long start = System.currentTimeMillis();

        Thread.currentThread().setName(tableName);
        LOGGER.debug("Starting table {}", tableName);

        Connection connection = null;

        final String fileName = context.getPath() + '/' + tableName + ".csv";
        try (FileToDBPipeline dataPipeline = new FileToDBPipeline(fileName)) {
            connection = context.get(PoolType.DESTINATION);
            if (!isOracle(connection)) {
                connection.setSchema(context.getDstSchema());
            }

            long rowCount = getRowCount(fileName);

            if (rowCount > context.getPageSize()) {
                parallelImport(connection, dataPipeline, tableName, rowCount);
            } else {
                singleImport(connection, dataPipeline, tableName, rowCount);
            }

            LOGGER.debug("Finished table {}: {} rows in {} ms", tableName, format.format(rowCount), format.format(System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            release(connection, PoolType.DESTINATION);
        }
    }

    private long getRowCount(String fileName) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), UTF_8))) {
            return reader.lines().count();
        }
    }

    private void parallelImport(Connection connection, FileToDBPipeline dataPipeline, String tableName, long rowCount) throws InterruptedException, SQLException {
        int pageCount = (int) Math.ceil(rowCount / (double) context.getPageSize());

        LOGGER.debug("Import {} pages", pageCount);

        List<PageBlockData> pages = new ArrayList<>(pageCount);

        int threadCount = Math.min(Constants.PARALLEL_PAGES, pageCount);
        for (int i = 0; i < threadCount; i++) {
            pages.add(new PageBlockData(i, 0, rowCount));
        }

        context.setPages(tableName, pages);

        final ResultSetMetaData metaData = getMetaData(connection, tableName);
        final String insertSql = SQLStatementFactory.getInsertValues(connection, context.getDstSchema(), tableName, metaData.getColumnCount());

        final Collection<Callable<Integer>> importThreads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            importThreads.add(new PageImportProcessor(context, tableName, metaData, dataPipeline, insertSql));
        }

        final List<Future<Integer>> results = context.getExecutor().invokeAll(importThreads);

        checkResults(tableName, threadCount, results);
    }

    private void singleImport(Connection connection, FileToDBPipeline dataPipeline, String tableName, long rowCount) throws SQLException, IOException {
        final double step = context.getPageSize() / 100.0;

        final ResultSetMetaData metaData = getMetaData(connection, tableName);
        final String sql = SQLStatementFactory.getInsertValues(connection, context.getDstSchema(), tableName, metaData.getColumnCount());
        final PreparedStatement statement = connection.prepareStatement(sql);

        while (dataPipeline.next(statement, metaData)) {
            if (dataPipeline.counter() % step == 0) {
                statement.executeBatch();
                connection.commit();
                dataPipeline.closeBatch();

                final long percent = Math.round(dataPipeline.counter() / rowCount * 100.0);
                LOGGER.debug("\t{}%", percent);
            }
        }

        statement.executeBatch();
        connection.commit();
        dataPipeline.closeBatch();
    }

    /**
     * Intentionally left resources opened. Not all drivers allow to work with ResultSetMetaData after statement closed.
     * @param connection
     * @param tableName
     * @return
     * @throws SQLException
     */
    @SuppressWarnings({"JDBCResourceOpenedButNotSafelyClosed", "resource"})
    private ResultSetMetaData getMetaData(Connection connection, String tableName) throws SQLException {
        final String selectAll = SQLStatementFactory.getSelectAll(connection, context.getDstSchema(), tableName);
        final ResultSet resultSet = connection.createStatement().executeQuery(selectAll);
        return resultSet.getMetaData();
    }
}
