package com.koldyr.csv.io;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.ResultSet;
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
import org.postgresql.core.Oid;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Pipeline to load data from db table into csv file. Dlob/clob columns will be stored in dedicated sub-folder
 *
 * @created: 2018.03.05
 */
public class DBToFilePipeline extends BaseDBPipeline implements Closeable {

    public static final String BLOB_FILE_EXT = ".bin";

    private static final Pattern CARRIAGE_RETURN = Pattern.compile("[\n\r]+");
    private static final String CR_REPLACEMENT = "\\\\n";

    private final BufferedWriter output;

    private File blobDir;
    private final File csvFile;

    public DBToFilePipeline(String fileName) throws IOException {
        super();

        csvFile = new File(fileName);
        final File dir = csvFile.getParentFile();
        if (!dir.exists()) {
            dir.mkdirs();
        }
        output = Files.newBufferedWriter(csvFile.toPath(), UTF_8);
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
        int columnType = getColumnType(resultSet.getMetaData(), columnIndex);

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
            case Types.CLOB:
            case Types.NCLOB:
                return saveCharStream(resultSet, columnIndex);
            case Types.BLOB:
            case Types.BINARY:
            case Oid.TEXT:
                return saveBinaryStream(resultSet, columnIndex);
            default:
                return StringUtils.EMPTY;
        }
    }

    private String saveBinaryStream(ResultSet resultSet, int columnIndex) throws SQLException {
        if (blobDir == null) {
            createBlobSubFolder();
        }

        final String blobId = UUID.randomUUID().toString();
        final File blobFile = new File(blobDir, blobId + BLOB_FILE_EXT);
        try (InputStream inputStream = resultSet.getBinaryStream(columnIndex);
             OutputStream outputStream = new FileOutputStream(blobFile)) {
            IOUtils.copy(inputStream, outputStream);
            outputStream.flush();
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return blobId;
    }

    private String saveCharStream(ResultSet resultSet, int columnIndex) throws SQLException {
        if (blobDir == null) {
            createBlobSubFolder();
        }

        final String blobId = UUID.randomUUID().toString();
        final File blobFile = new File(blobDir, blobId + BLOB_FILE_EXT);
        try (Reader reader = resultSet.getCharacterStream(columnIndex);
             OutputStream outputStream = new FileOutputStream(blobFile)) {
            IOUtils.copy(reader, outputStream, UTF_8);
            outputStream.flush();
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return blobId;
    }

    private void createBlobSubFolder() {
        blobDir = new File(csvFile.getParentFile(), stripExtension(csvFile));
        if (!blobDir.exists()) {
            blobDir.mkdirs();
        }
    }

    static String stripExtension(File csvFile) {
        String name = csvFile.getName();
        int dotIndex = name.lastIndexOf('.');
        return name.substring(0, dotIndex);
    }
}
