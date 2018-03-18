/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.io;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Description of class DbToDBPipeline
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
        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
            final int columnType = metaData.getColumnType(columnIndex);
            if (isString(columnType)) {
                final String value = source.getString(columnIndex);
                destination.setString(columnIndex, value);
            } else {
                final Object value = source.getObject(columnIndex);
                destination.setObject(columnIndex, value, columnType);
            }
        }
    }

    private boolean isString(int columnType) {
        return columnType == Types.VARCHAR || columnType == Types.NVARCHAR || columnType == Types.NCHAR || columnType == Types.CHAR;
    }
}
