package com.koldyr.csv.db;

/**
 * Description of class PlainSQLGenerator
 *
 * @created: 2018-09-25
 */
public class PlainSQLGenerator implements SQLGenerator {
    @Override
    public String getSelectAll(String schema, String tableName) {
        return String.format("SELECT * FROM %s.%s", schema, tableName);
    }

    @Override
    public String getInsertValues(String schema, String tableName, String values) {
        return String.format("INSERT INTO %s.%s VALUES (%s)", schema, tableName, values);
    }

    @Override
    public String getRowCount(String schema, String tableName) {
        return String.format("SELECT count(1) FROM %s.%s", schema, tableName);
    }
}
