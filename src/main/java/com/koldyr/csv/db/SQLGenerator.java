package com.koldyr.csv.db;

/**
 * Description of class SQLGenerator
 *
 * @created: 2018-09-25
 */
public interface SQLGenerator {
    String getSelectAll(String schema, String tableName);

    String getInsertValues(String schema, String tableName, String values);

    String getRowCount(String schema, String tableName);
}
