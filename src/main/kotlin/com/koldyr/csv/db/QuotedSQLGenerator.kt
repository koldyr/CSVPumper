package com.koldyr.csv.db

import org.apache.commons.lang.StringUtils

/**
 * Description of class QuotedSQLGenerator
 *
 * @created: 2018-09-25
 */
class QuotedSQLGenerator : SQLGenerator {
    override fun getSelectAll(schema: String, tableName: String): String {
        return if (StringUtils.isEmpty(schema)) {
            "SELECT * FROM \"$tableName\""
        } else "SELECT * FROM \"$schema\".\"$tableName\""
    }

    override fun getInsertValues(schema: String, tableName: String, values: String): String {
        return if (StringUtils.isEmpty(schema)) {
            "INSERT INTO \"$tableName\" VALUES ($values)"
        } else "INSERT INTO \"$schema\".\"$tableName\" VALUES ($values)"
    }

    override fun getRowCount(schema: String, tableName: String): String {
        return if (StringUtils.isEmpty(schema)) {
            "SELECT count(1) FROM \"$tableName\""
        } else "SELECT count(1) FROM \"$schema\".\"$tableName\""
    }
}
