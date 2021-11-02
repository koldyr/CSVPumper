package com.koldyr.csv.db

import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData
import com.mysql.cj.jdbc.JdbcConnection
import oracle.jdbc.OracleConnection
import oracle.jdbc.OracleResultSet
import org.postgresql.core.BaseConnection
import org.postgresql.jdbc.PgResultSetMetaData
import org.postgresql.jdbc.PgStatement
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Statement

/**
 * Description of class DatabaseDetector
 *
 * @created: 2018.09.08
 */
object DatabaseDetector {

    fun isOracle(resultSet: ResultSet?): Boolean {
        return resultSet is OracleResultSet
    }

    fun isOracle(connection: Connection?): Boolean {
        return connection is OracleConnection
    }

    fun isPostgreSQL(connection: Connection?): Boolean {
        return connection is BaseConnection
    }

    fun isMySql(connection: Connection?): Boolean {
        return connection is JdbcConnection
    }

    fun isH2Sql(connection: Connection?): Boolean {
        return connection is org.h2.jdbc.JdbcConnection
    }

    fun isMsSQLServer(connection: Connection?): Boolean {
        return connection is ISQLServerConnection
    }

    fun isCLOBSupported(statement: Statement?): Boolean {
        return statement !is PgStatement
    }

    fun isBLOBSupported(statement: Statement?): Boolean {
        return statement !is PgStatement
    }

    fun isMsSQLServer(metaData: ResultSetMetaData?): Boolean {
        return metaData is SQLServerResultSetMetaData
    }

    fun isPostgreSQL(metaData: ResultSetMetaData?): Boolean {
        return metaData is PgResultSetMetaData
    }
}
