package com.koldyr.csv.io;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Description of class DataTransfer
 *
 * @created: 2018.03.05
 */
public class DBToFilePipeline implements Closeable {
    private static final byte[] CR = "\n".getBytes();

    private final ReadWriteLock writeLock = new ReentrantReadWriteLock();

    private final FileChannel fileChannel;
    private final FileOutputStream output;

    public DBToFilePipeline(String fileName) throws FileNotFoundException {
        output = new FileOutputStream(fileName);
        fileChannel = output.getChannel();
    }

    public void flush() throws IOException {
        fileChannel.force(false);
    }

    @Override
    public void close() throws IOException {
        fileChannel.force(true);
        fileChannel.close();

        output.flush();
        output.close();
    }

    public boolean next(ResultSet resultSet, int columnCount) throws SQLException, IOException {
        if (resultSet.next()) {
            final ByteBuffer data = composeRowData(resultSet, columnCount);

            final Lock lock = writeLock.writeLock();
            lock.lock();
            try {
                writeChannel(data);
            } finally {
                lock.unlock();
            }
            return true;
        }

        return false;
    }

    private void writeChannel(ByteBuffer data) throws IOException {
        try (FileLock lock = fileChannel.lock(fileChannel.position(), data.capacity() + 1, false)) {
            fileChannel.write(data);
            fileChannel.write(ByteBuffer.wrap(CR));
        }
    }

    private ByteBuffer composeRowData(ResultSet resultSet, int columnCount) throws SQLException, UnsupportedEncodingException {
        StringJoiner rowData = new StringJoiner(",");
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            Object value = getValue(resultSet, columnIndex);
            rowData.add(value == null ? StringUtils.EMPTY : StringEscapeUtils.escapeCsv(value.toString()));
        }

        return ByteBuffer.wrap(rowData.toString().getBytes("UTF-8"));
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

                value = value.trim();
                return value;
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
