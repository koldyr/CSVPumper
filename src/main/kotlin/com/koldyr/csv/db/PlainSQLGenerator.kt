package com.koldyr.csv.db

import org.apache.commons.lang3.StringUtils.isEmpty

/**
 * Description of class PlainSQLGenerator
 *
 * @created: 2018-09-25
 */
class PlainSQLGenerator : SQLGenerator {
    override fun getSelectAll(schema: String, tableName: String): String {
        return if (isEmpty(schema)) {
            "SELECT * FROM $tableName"
        } else "SELECT * FROM $schema.$tableName"
    }

    override fun getInsertValues(schema: String, tableName: String, values: String): String {
        return if (isEmpty(schema)) {
            "INSERT INTO $tableName VALUES ($values)"
        } else "INSERT INTO $schema.$tableName VALUES ($values)"
    }

    override fun getRowCount(schema: String, tableName: String): String {
        return if (isEmpty(schema)) {
            "SELECT count(1) FROM $tableName"
        } else "SELECT count(1) FROM $schema.$tableName"
    }
}
