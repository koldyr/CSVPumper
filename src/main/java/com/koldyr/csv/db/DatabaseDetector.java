package com.koldyr.csv.db;

import java.sql.Connection;
import java.sql.ResultSet;

import org.postgresql.core.BaseConnection;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;

import com.mysql.cj.api.jdbc.JdbcConnection;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;

/**
 * Description of class DatabaseDetector
 *
 * @created: 2018.09.08
 */
public class DatabaseDetector {

    public static boolean isOracle(ResultSet resultSet) {
        return resultSet instanceof OracleResultSet;
    }

    public static boolean isOracle(Connection connection) {
        return connection instanceof OracleConnection;
    }

    public static boolean isPostgreSQL(Connection connection) {
        return connection instanceof BaseConnection;
    }

    public static boolean isMySql(Connection connection) {
        return connection instanceof JdbcConnection;
    }

    public static boolean isMsSQLServer(Connection connection) {
        return connection instanceof ISQLServerConnection;
    }
}
