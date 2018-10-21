package com.koldyr.csv.io;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Pipeline to load data from one table to directly another database , w/o intermediate file
 *
 * @created: 2018.03.18
 */
public class DbToDbPipeline extends BaseDBPipeline {

    public boolean next(ResultSet source, PreparedStatement destination) throws SQLException {
        if (source.next()) {
            try {
                copyValues(source, destination);
                destination.addBatch();
            } catch (Exception e) {
                throw new SQLException("Row num " + source.getRow(), e);
            }

            return true;
        }

        return false;
    }

    private void copyValues(ResultSet source, PreparedStatement destination) throws SQLException {
        ResultSetMetaData srcMetaData = source.getMetaData();
        int columnCount = srcMetaData.getColumnCount();

        if (hasNonDataColumns(source)) {
            columnCount--;
        }

        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            final int columnType = getColumnType(srcMetaData, columnIndex);

            if (isString(columnType)) {
                final String value = source.getString(columnIndex);
                destination.setString(columnIndex, value);
            } else if (isLOB(columnType)) {
                final InputStream lob = source.getBinaryStream(columnIndex);
                setLOB(destination, columnIndex, columnType, lob);
            } else if (columnType == Types.TIMESTAMP) {
                final Timestamp timestamp = source.getTimestamp(columnIndex);
                destination.setTimestamp(columnIndex, timestamp);
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
}
