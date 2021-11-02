package com.koldyr.csv.db

/**
 * Description of class SQLGenerator
 *
 * @created: 2018-09-25
 */
interface SQLGenerator {
    fun getSelectAll(schema: String, tableName: String): String

    fun getInsertValues(schema: String, tableName: String, values: String): String

    fun getRowCount(schema: String, tableName: String): String
}
