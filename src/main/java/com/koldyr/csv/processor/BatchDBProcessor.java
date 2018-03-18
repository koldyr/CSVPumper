package com.koldyr.csv.processor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.LoggerFactory;

import com.koldyr.csv.model.PoolType;
import com.koldyr.csv.model.ProcessorContext;

/**
 * Description of class DBProcessor
 *
 * @created: 2018.03.03
 */
public abstract class BatchDBProcessor implements Callable<Object> {

    protected final DecimalFormat format;
    protected final ProcessorContext context;

    protected BatchDBProcessor(ProcessorContext context) {
        this.context = context;

        final DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setGroupingSeparator(',');
        format = new DecimalFormat("###,###,###,###", decimalFormatSymbols);
    }

    @Override
    public Object call() {
        String tableName = context.getNextTable();
        while (tableName != null) {
            processTable(tableName);
            tableName = context.getNextTable();
        }

        return null;
    }

    protected abstract void processTable(String tableName);

    protected void checkResults(String tableName, int pageCount, Collection<Future<Integer>> results) throws InterruptedException {
        final int count = results.stream().mapToInt(result -> {
            try {
                return result.get();
            } catch (Exception e) {
                return 0;
            }
        }).sum();

        LoggerFactory.getLogger(getClass()).debug("checkResults - Expected: {}, Actual: {}", pageCount, count);

        if (count != pageCount) {
            throw new InterruptedException("Error exporting " + tableName);
        }
    }

    protected long getRowCount(Connection connection, String tableName) throws SQLException {
        long rowCount = 0;

        ResultSet resultSet = null;
        try (Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery("SELECT count(1) FROM " + context.getSchema() + '.' + tableName);

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

    protected void release(Connection connection, PoolType type) {
        if (connection != null) {
            try {
                context.release(type, connection);
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).error(e.getMessage(), e);
            }
        }
    }

    public static String createInsertSql(String schema, String tableName, int columnCount) {
        StringJoiner values = new StringJoiner(",");
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            values.add("?");
        }

        return "INSERT INTO \"" + schema + "\".\"" + tableName + "\" VALUES (" + values + ')';
    }
}
