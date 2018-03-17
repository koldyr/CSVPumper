package com.koldyr.csv.io;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description of class DataTransfer
 *
 * @created: 2018.03.05
 */
public class DBToFilePipeline implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBToFilePipeline.class);

    private static final Pattern CARRIAGE_RETURN = Pattern.compile("[\n\r]+");
    private static final String CR_REPLACEMENT = "\\\\n";

    private final BufferedWriter output;

    public DBToFilePipeline(String fileName) throws FileNotFoundException {
        output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true)));
    }

    public void flush() throws IOException {
        output.flush();
    }

    @Override
    public void close() throws IOException {
        output.flush();
        output.close();
    }

    public boolean next(ResultSet resultSet, int columnCount) throws SQLException, IOException {
        if (resultSet.next()) {
            output.write(composeRowData(resultSet, columnCount));
            return true;
        }

        return false;
    }

    private String composeRowData(ResultSet resultSet, int columnCount) throws SQLException {
        StringJoiner rowData = new StringJoiner(",", "", "\n");
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            Object value = getValue(resultSet, columnIndex);
            rowData.add(value == null ? StringUtils.EMPTY : StringEscapeUtils.escapeCsv(value.toString()));
        }

        return rowData.toString();
    }

    private Object getValue(ResultSet resultSet, int columnIndex) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnType = metaData.getColumnType(columnIndex);

        switch (columnType) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.CHAR:
                String value = resultSet.getString(columnIndex);
                if (value == null) return null;

                final Matcher carriageReturn = CARRIAGE_RETURN.matcher(value);
                if (carriageReturn.find()) {
                    value = carriageReturn.replaceAll(CR_REPLACEMENT);
                }
                return value.trim();
            case Types.INTEGER:
                return resultSet.getInt(columnIndex);
            case Types.FLOAT:
                return resultSet.getFloat(columnIndex);
            case Types.DATE:
                final Date date = resultSet.getDate(columnIndex);
                if (date == null) return null;

                return DateTimeFormatter.ISO_LOCAL_DATE.format(date.toLocalDate());
            case Types.TIMESTAMP:
                final Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                if (timestamp == null) return null;

                return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(timestamp.toLocalDateTime());
            case Types.NUMERIC:
                return resultSet.getBigDecimal(columnIndex);
            default:
                return StringUtils.EMPTY;
        }
    }
}
