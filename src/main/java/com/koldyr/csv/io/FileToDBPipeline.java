package com.koldyr.csv.io;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.postgresql.core.Oid;

import static com.koldyr.csv.io.DBToFilePipeline.BLOB_FILE_EXT;
import static com.koldyr.csv.io.DBToFilePipeline.stripExtension;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Pipeline to load data from cvs file to database table
 *
 * @created: 2018.03.07
 */
public class FileToDBPipeline extends BaseDBPipeline implements Closeable {

    private static final Pattern CARRIAGE_RETURN = Pattern.compile("\\\\n");
    private static final String CR_REPLACEMENT = "\n";

    private final ReadWriteLock fileLock = new ReentrantReadWriteLock();

    private final BufferedReader reader;

    private final AtomicLong counter = new AtomicLong();

    private final File blobDir;

    private final Collection<InputStream> blobStreams = new LinkedList<>();

    public FileToDBPipeline(String fileName) throws FileNotFoundException {
        final File csvFile = new File(fileName);

        blobDir = new File(csvFile.getParentFile(), stripExtension(csvFile));
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), UTF_8));
    }

    public long counter() {
        return counter.longValue();
    }

    public boolean next(PreparedStatement statement, ResultSetMetaData metaData) throws SQLException, IOException {
        String rowData = readLine();
        if (rowData == null) {
            return false;
        }

        try {
            setRowValues(statement, metaData, rowData);
            statement.addBatch();
        } catch (Exception e) {
            throw new SQLException(rowData, e);
        }

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
        int columnType = getColumnType(metaData, columnIndex);

        if (isString(columnType)) {
            if (value == null) {
                statement.setNull(columnIndex, columnType);
                return;
            }
        } else {
            if (StringUtils.isEmpty(value)) {
                statement.setNull(columnIndex, columnType);
                return;
            }
        }

        String v = StringEscapeUtils.unescapeCsv(value);
        final Matcher carriageReturn = CARRIAGE_RETURN.matcher(v);
        if (carriageReturn.find()) {
            v = carriageReturn.replaceAll(CR_REPLACEMENT);
        }

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
            case Types.BIGINT:
                statement.setLong(columnIndex, Long.parseLong(v));
                break;
            case Types.FLOAT:
                statement.setFloat(columnIndex, Float.parseFloat(v));
                break;
            case Types.DATE:
                LocalDate date;
                try {
                    date = LocalDate.parse(v, DateTimeFormatter.ISO_LOCAL_DATE);
                } catch (DateTimeParseException e) {
                    date = LocalDate.parse(v, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                }
                statement.setDate(columnIndex, Date.valueOf(date));
                break;
            case Types.TIMESTAMP:
                final LocalDateTime dateTime = LocalDateTime.parse(v, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                statement.setTimestamp(columnIndex, Timestamp.valueOf(dateTime));
                break;
            case Types.NUMERIC:
                statement.setBigDecimal(columnIndex, new BigDecimal(v));
                break;
            case Types.BLOB:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.BINARY:
            case Oid.TEXT:
                setLOB(statement, columnIndex, value, columnType);
                break;
            default:
                statement.setObject(columnIndex, v, columnType);
        }
    }

    private void setLOB(PreparedStatement statement, int columnIndex, String blobId, int columnType) throws SQLException {
        try {
            final InputStream lob = new FileInputStream(new File(blobDir, blobId + BLOB_FILE_EXT));
            blobStreams.add(lob);

            setLOB(statement, columnIndex, columnType, lob);
        } catch (FileNotFoundException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public void closeBatch() {
        blobStreams.forEach(inputStream -> {
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        blobStreams.clear();
    }
}
