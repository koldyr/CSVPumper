package com.koldyr.csv.io;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Description of class DataTransfer
 *
 * @created: 2018.03.05
 */
public class DBToFilePipeline implements Closeable {

    public static final String BLOB_FILE_EXT = ".bin";

    private static final Pattern CARRIAGE_RETURN = Pattern.compile("[\n\r]+");
    private static final String CR_REPLACEMENT = "\\\\n";

    private final BufferedWriter output;
    private final File blobDir;

    public DBToFilePipeline(String fileName) throws FileNotFoundException {
        final File csvFile = new File(fileName);
        csvFile.mkdirs();
        output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile, true), UTF_8));

        final File dir = csvFile.getParentFile();
        blobDir = new File(dir.getAbsolutePath() + '/' + stripExtension(csvFile) + '/');
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
            case Types.BLOB:
            case Types.CLOB:
            case Types.NCLOB:
                return saveStream(resultSet, columnIndex);
            default:
                return StringUtils.EMPTY;
        }
    }

    private String saveStream(ResultSet resultSet, int columnIndex) throws SQLException {
        final String blobId = UUID.randomUUID().toString();
        try (InputStream inputStream = resultSet.getBinaryStream(columnIndex);
             OutputStream outputStream = new FileOutputStream(new File(blobDir, blobId + BLOB_FILE_EXT))) {
            IOUtils.copy(inputStream, outputStream);
            outputStream.flush();
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return blobId;
    }

    public static String stripExtension(File csvFile) {
        String name = csvFile.getName();
        int dotIndex = name.lastIndexOf('.');
        return name.substring(0, dotIndex);
    }
}
