package com.koldyr.csv.io;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Pipeline to load data from one table to directly another database , w/o intermediate file
 *
 * @created: 2018.03.18
 */
public class DbToDbPipeline {

    public boolean next(ResultSet source, PreparedStatement destination) throws SQLException {
        if (source.next()) {
            try {
                copyValues(source, destination);
            } catch (Exception e) {
                throw new SQLException("Row num " + source.getRow(), e);
            }

            destination.addBatch();

            return true;
        }

        return false;
    }

    private void copyValues(ResultSet source, PreparedStatement destination) throws SQLException {
        ResultSetMetaData metaData = source.getMetaData();
        int columnCount = metaData.getColumnCount();

        if (hasNonDataColumns(source)) {
            columnCount--;
        }

        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            final int columnType = metaData.getColumnType(columnIndex);
            if (isString(columnType)) {
                final String value = source.getString(columnIndex);
                destination.setString(columnIndex, value);
            } else if (isBlob(columnType)) {
                final InputStream value = source.getBinaryStream(columnIndex);
                destination.setBinaryStream(columnIndex, value);
            } else {
                final Object value = source.getObject(columnIndex);
                destination.setObject(columnIndex, value, columnType);
            }
        }
    }

    private boolean hasNonDataColumns(ResultSet resultSet) {
        try {
            return resultSet.findColumn("RNUM") > -1;
        } catch (SQLException e) {
            return false;
        }
    }

    private boolean isString(int columnType) {
        return columnType == Types.VARCHAR || columnType == Types.NVARCHAR || columnType == Types.NCHAR || columnType == Types.CHAR;
    }

    private boolean isBlob(int columnType) {
        return columnType == Types.BLOB || columnType == Types.CLOB || columnType == Types.NCLOB || columnType == Types.LONGNVARCHAR;
    }
}
