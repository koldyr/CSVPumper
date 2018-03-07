package com.koldyr.csv.processor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.model.ProcessorContext;

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
        LOGGER.debug("Starting {}", tableName);

        BufferedReader reader = null;
        PreparedStatement statement = null;
        try {
            final Connection connection = context.getConnection();
            connection.setSchema(context.getSchema());

            reader = new BufferedReader(new InputStreamReader(new FileInputStream(context.getPath() + '/' + tableName + ".csv"), "UTF-8"));

            ResultSetMetaData metaData = getMetaData(connection, tableName);
            String sql = createInsertSql(tableName, metaData);
            statement = connection.prepareStatement(sql);

            String rowData = reader.readLine();
            long counter = 0;
            while (rowData != null) {
                insertRow(statement, metaData, rowData);

                counter++;

                if (counter % 1000.0 == 0) {
                    statement.executeBatch();
                    LOGGER.debug("\t{}", format.format(counter));
                }

                rowData = reader.readLine();
            }

            statement.executeBatch();

            LOGGER.debug("Finished {}: {} rows in {} msec", tableName, format.format(counter), format.format(System.currentTimeMillis() - start));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (statement != null) {
                    statement.close();
                }
            } catch (Exception e) {
                LOGGER.error(tableName + ": " + e.getMessage(), e);
            }
        }
    }

    private String createInsertSql(String tableName, ResultSetMetaData metaData) throws SQLException {
        StringJoiner values = new StringJoiner(",");
        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
            values.add("?");
        }

        return "INSERT INTO \"" + context.getSchema() + "\".\"" + tableName + "\" VALUES (" + values + ')';
    }

    private void insertRow(PreparedStatement statement, ResultSetMetaData metaData, String rowData) throws SQLException {
        final String[] values = rowData.split(",");
        for (int columnIndex = 1; columnIndex <= values.length; columnIndex++) {
            String value = values[columnIndex - 1];
            setValue(metaData, statement, columnIndex, value);
        }

        statement.addBatch();
    }

    private void setValue(ResultSetMetaData metaData, PreparedStatement statement, int columnIndex, String value) throws SQLException {
        final int columnType = metaData.getColumnType(columnIndex);
        if (StringUtils.isEmpty(value)) {
            statement.setNull(columnIndex, columnType);
            return;
        }

        switch (columnType) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.CHAR:
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    statement.setString(columnIndex, value.substring(1, value.length() - 1));
                } else {
                    statement.setString(columnIndex, value);
                }
                break;
            case Types.INTEGER:
                statement.setInt(columnIndex, Integer.parseInt(value));
                break;
            case Types.FLOAT:
                statement.setFloat(columnIndex, Float.parseFloat(value));
            case Types.DATE:
                final LocalDate date = (LocalDate) DateTimeFormatter.ISO_LOCAL_DATE.parse(value);
                statement.setDate(columnIndex, Date.valueOf(date));
                break;
            case Types.TIMESTAMP:
                final LocalDateTime dateTime = (LocalDateTime) DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(value);
                statement.setTimestamp(columnIndex, Timestamp.valueOf(dateTime));
            case Types.NUMERIC:
                statement.setBigDecimal(columnIndex, new BigDecimal(value));
            default:
                statement.setObject(columnIndex, value, columnType);
        }
    }

    private ResultSetMetaData getMetaData(Connection connection, String tableName) throws SQLException {
        ResultSet resultSet = null;
        try (Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery("SELECT * FROM \"" + context.getSchema() + "\".\"" + tableName + '"');
            return resultSet.getMetaData();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }
}
