/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.io;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Description of class FileToDBPipeline
 *
 * @created: 2018.03.07
 */
public class FileToDBPipeline implements Closeable {

    private final ReadWriteLock fileLock = new ReentrantReadWriteLock();

    private final BufferedReader reader;

    private final AtomicLong counter = new AtomicLong();

    public FileToDBPipeline(String fileName) throws FileNotFoundException, UnsupportedEncodingException {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
    }

    public long counter() {
        return counter.longValue();
    }

    public boolean next(PreparedStatement statement, ResultSetMetaData metaData) throws SQLException, IOException {
        String rowData = readLine();
        if (rowData == null) {
            return false;
        }

        setRowValues(statement, metaData, rowData);

        statement.addBatch();

        return true;
    }

    private String readLine() throws IOException {
        final Lock lock = fileLock.readLock();
        lock.lock();
        try {
            counter.incrementAndGet();
            return reader.readLine();
        } finally {
            lock.unlock();
        }
    }

    private void setRowValues(PreparedStatement statement, ResultSetMetaData metaData, String rowData) throws SQLException {
        boolean escaped = false;
        int columnIndex = 1;
        CharArrayWriter tokenBuffer = new CharArrayWriter(256);
        final CharBuffer rowBuffer = CharBuffer.wrap(rowData);
        while (rowBuffer.hasRemaining()) {
            final char c = rowBuffer.get();

            if (escaped) {
                if (c == '"') {
                    escaped = false;
                }
            } else {
                escaped = c == '"';
            }

            if (c == ',' && !escaped) {
                setValue(metaData, statement, columnIndex, tokenBuffer.toString());

                tokenBuffer.reset();
                columnIndex++;

                continue;
            }

            tokenBuffer.append(c);
        }

        setValue(metaData, statement, columnIndex, tokenBuffer.toString());
    }

    private void setValue(ResultSetMetaData metaData, PreparedStatement statement, int columnIndex, String value) throws SQLException {
        final int columnType = metaData.getColumnType(columnIndex);
        if (StringUtils.isEmpty(value)) {
            statement.setNull(columnIndex, columnType);
            return;
        }

        String v = StringEscapeUtils.unescapeCsv(value);
        switch (columnType) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
            case Types.CHAR:
                statement.setString(columnIndex, v);
                break;
            case Types.INTEGER:
                statement.setInt(columnIndex, Integer.parseInt(v));
                break;
            case Types.FLOAT:
                statement.setFloat(columnIndex, Float.parseFloat(v));
            case Types.DATE:
                final LocalDate date = (LocalDate) DateTimeFormatter.ISO_LOCAL_DATE.parse(v);
                statement.setDate(columnIndex, Date.valueOf(date));
                break;
            case Types.TIMESTAMP:
                final LocalDateTime dateTime = (LocalDateTime) DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(v);
                statement.setTimestamp(columnIndex, Timestamp.valueOf(dateTime));
            case Types.NUMERIC:
                statement.setBigDecimal(columnIndex, new BigDecimal(v));
            default:
                statement.setObject(columnIndex, v, columnType);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
