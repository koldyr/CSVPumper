package com.koldyr.csv.io;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.core.Oid;

import static com.koldyr.csv.db.DatabaseDetector.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Description of class BaseDBPipeline
 *
 * @created: 2018.09.24
 */
public abstract class BaseDBPipeline {
    protected boolean isString(int columnType) {
        return columnType == Types.VARCHAR || columnType == Types.NVARCHAR || columnType == Types.NCHAR || columnType == Types.CHAR;
    }

    protected boolean isLOB(int columnType) {
        return columnType == Types.BLOB || columnType == Types.CLOB || columnType == Types.NCLOB || columnType == Types.BINARY || columnType == Types.LONGVARBINARY
                || columnType == Oid.TEXT;
    }

    protected void setLOB(PreparedStatement destination, int columnIndex, int columnType, InputStream lob) throws SQLException {
        switch (columnType) {
            case Types.BLOB:
                if (isBLOBSupported(destination)) {
                    destination.setBlob(columnIndex, lob);
                } else {
                    destination.setBinaryStream(columnIndex, lob);
                }
                break;
            case Types.CLOB:
                if (isCLOBSupported(destination)) {
                    destination.setClob(columnIndex, new InputStreamReader(lob, UTF_8));
                } else {
                    destination.setCharacterStream(columnIndex, new InputStreamReader(lob, UTF_8));
                }
                break;
            case Types.NCLOB:
                destination.setNClob(columnIndex, new InputStreamReader(lob, UTF_8));
                break;
            case Types.BINARY:
            case Types.LONGVARBINARY:
                destination.setBinaryStream(columnIndex, lob);
                break;
            case Oid.TEXT:
                destination.setCharacterStream(columnIndex, new InputStreamReader(lob, UTF_8));
                break;
            default:
        }
    }

    protected int getColumnType(ResultSetMetaData metaData, int columnIndex) throws SQLException {
        int columnType = metaData.getColumnType(columnIndex);

        if (isString(columnType)) {
            if (isPostgreSQL(metaData)) {
                final String columnTypeName = metaData.getColumnTypeName(columnIndex);
                if (columnTypeName.equals("text")) {
                    columnType = Oid.TEXT;
                }
            } else if (isMsSQLServer(metaData)) {
                final int precision = metaData.getPrecision(columnIndex);
                if (columnType == Types.VARCHAR) {
                    if (precision == 2_147_483_647) {
                        return Types.CLOB;
                    }
                } else if (columnType == Types.NVARCHAR) {
                    if (precision == 1_073_741_823) {
                        return Types.NCLOB;
                    }
                }
            }
        }
        return columnType;
    }
}
