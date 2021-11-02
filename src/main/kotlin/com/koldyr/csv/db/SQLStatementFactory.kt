package com.koldyr.csv.db

import com.koldyr.csv.db.DatabaseDetector.isH2Sql
import com.koldyr.csv.db.DatabaseDetector.isMsSQLServer
import com.koldyr.csv.db.DatabaseDetector.isMySql
import com.koldyr.csv.db.DatabaseDetector.isOracle
import com.koldyr.csv.db.DatabaseDetector.isPostgreSQL
import com.koldyr.csv.model.PageBlockData
import java.sql.Connection
import java.util.*

/**
 * Description of class SQLStatementFactory
 *
 * @created: 2018-09-25
 */
object SQLStatementFactory {
    private val quotedSqlGenerator = QuotedSQLGenerator()
    private val plainSqlGenerator = PlainSQLGenerator()

    private fun selectGenerator(connection: Connection): SQLGenerator {
        val quoted = isPostgreSQL(connection) || isMsSQLServer(connection)
        return if (quoted) quotedSqlGenerator else plainSqlGenerator
    }

    fun getSelectAll(connection: Connection, schema: String, tableName: String): String {
        val generator = selectGenerator(connection)

        return generator.getSelectAll(schema, tableName)
    }

    fun getInsertValues(connection: Connection, schema: String, tableName: String, columnCount: Int): String {
        val generator = selectGenerator(connection)

        val values = StringJoiner(",")
        for (columnIndex in 1..columnCount) {
            values.add("?")
        }

        return generator.getInsertValues(schema, tableName, values.toString())
    }

    fun getRowCount(connection: Connection, schema: String, tableName: String): String {
        val generator = selectGenerator(connection)

        return generator.getRowCount(schema, tableName)
    }

    fun getPageSQL(connection: Connection, pageBlock: PageBlockData, schema: String, tableName: String): String {
        val oracle = isOracle(connection)
        if (oracle) {
            return "SELECT * FROM (SELECT subQ.*, rownum RNUM FROM ( SELECT * FROM " + schema + '.' + tableName +
                    " ORDER BY 1) subQ WHERE rownum <= " + (pageBlock.start + pageBlock.length) + " ORDER BY 1) WHERE RNUM > " + pageBlock.start + " ORDER BY 1"
        }

        val msSQLServer = isMsSQLServer(connection)
        if (msSQLServer) {
            return "SELECT * FROM $schema.$tableName ORDER BY 1 OFFSET ${pageBlock.start} ROWS FETCH NEXT ${pageBlock.length} ROWS ONLY"
        }

        val mySQL = isMySql(connection)
        val h2SQL = isH2Sql(connection)
        val postgreSQL = isPostgreSQL(connection)

        return if (postgreSQL || mySQL || h2SQL) {
            "SELECT * FROM $schema.$tableName ORDER BY 1 LIMIT ${pageBlock.length} OFFSET ${pageBlock.start}"
        } else "SELECT * FROM $schema.$tableName"
    }
}
