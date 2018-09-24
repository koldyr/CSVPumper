/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.io;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.postgresql.core.Oid;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Description of class BaseDBPipeline
 *
 * @created: 2018.09.24
 */
public class BaseDBPipeline {
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
                destination.setBlob(columnIndex, lob);
                break;
            case Types.CLOB:
                destination.setClob(columnIndex, new InputStreamReader(lob, UTF_8));
                break;
            case Types.NCLOB:
                destination.setNClob(columnIndex, new InputStreamReader(lob, UTF_8));
                break;
            case Types.BINARY:
                destination.setBinaryStream(columnIndex, lob);
                break;
            case Oid.TEXT:
                destination.setCharacterStream(columnIndex, new InputStreamReader(lob, UTF_8));
                break;
            default:
        }
    }

}
