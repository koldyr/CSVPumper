package com.koldyr.csv.db;

import java.sql.Connection;
import java.util.StringJoiner;

import static com.koldyr.csv.db.DatabaseDetector.*;

import com.koldyr.csv.model.PageBlockData;

/**
 * Description of class SQLStatementFactory
 *
 * @created: 2018-09-25
 */
public class SQLStatementFactory {
    static final SQLGenerator quotedSqlGenerator = new QuotedSQLGenerator();
    static final SQLGenerator plainSqlGenerator = new PlainSQLGenerator();

    private static SQLGenerator selectGenerator(Connection connection) {
        boolean quoted = isPostgreSQL(connection) || isMsSQLServer(connection);
        return quoted ? quotedSqlGenerator : plainSqlGenerator;
    }

    public static String getSelectAll(Connection connection, String schema, String tableName) {
        final SQLGenerator generator = selectGenerator(connection);

        return generator.getSelectAll(schema, tableName);
    }

    public static String getInsertValues(Connection connection, String schema, String tableName, int columnCount) {
        final SQLGenerator generator = selectGenerator(connection);

        final StringJoiner values = new StringJoiner(",");
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            values.add("?");
        }

        return generator.getInsertValues(schema, tableName, values.toString());
    }

    public static String getRowCount(Connection connection, String schema, String tableName) {
        final SQLGenerator generator = selectGenerator(connection);

        return generator.getRowCount(schema, tableName);
    }

    public static String getPageSQL(Connection connection, PageBlockData pageBlock, String schema, String tableName) {
        boolean oracle = isOracle(connection);
        if (oracle) {
            return "SELECT * FROM (SELECT subQ.*, rownum RNUM FROM ( SELECT * FROM " + schema + '.' + tableName +
                    " ORDER BY 1) subQ WHERE rownum <= " + (pageBlock.start + pageBlock.length) + " ORDER BY 1) WHERE RNUM > " + pageBlock.start + " ORDER BY 1";
        }

        boolean msSQLServer = isMsSQLServer(connection);
        if (msSQLServer) {
            return "SELECT * FROM " + schema + '.' + tableName + " ORDER BY 1 OFFSET " + pageBlock.start + " ROWS FETCH NEXT " + pageBlock.length + " ROWS ONLY";
        }

        boolean mySQL = isMySql(connection);
        boolean postgreSQL = isPostgreSQL(connection);
        if (postgreSQL || mySQL) {
            return "SELECT * FROM " + schema + '.' + tableName + " ORDER BY 1 LIMIT " + pageBlock.length + " OFFSET " + pageBlock.start;
        }

        return "SELECT * FROM " + schema + '.' + tableName;
    }
}
