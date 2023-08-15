package com.koldyr.csv.db

import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Statement
import oracle.jdbc.OracleConnection
import oracle.jdbc.OracleResultSet
import com.microsoft.sqlserver.jdbc.ISQLServerConnection
import com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData
import com.mysql.cj.jdbc.JdbcConnection
import org.postgresql.core.BaseConnection
import org.postgresql.jdbc.PgResultSetMetaData
import org.postgresql.jdbc.PgStatement

/**
 * Description of class DatabaseDetector
 *
 * @created: 2018.09.08
 */
object DatabaseDetector {

    fun isOracle(resultSet: ResultSet?): Boolean = resultSet is OracleResultSet

    fun isOracle(connection: Connection?): Boolean = connection is OracleConnection

    fun isPostgreSQL(connection: Connection?): Boolean = connection is BaseConnection

    fun isMySql(connection: Connection?): Boolean = connection is JdbcConnection

    fun isH2Sql(connection: Connection?): Boolean = connection is org.h2.jdbc.JdbcConnection

    fun isMsSQLServer(connection: Connection?): Boolean = connection is ISQLServerConnection

    fun isCLOBSupported(statement: Statement?): Boolean = statement !is PgStatement

    fun isBLOBSupported(statement: Statement?): Boolean = statement !is PgStatement

    fun isMsSQLServer(metaData: ResultSetMetaData?): Boolean = metaData is SQLServerResultSetMetaData

    fun isPostgreSQL(metaData: ResultSetMetaData?): Boolean = metaData is PgResultSetMetaData
}
