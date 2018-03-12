package com.koldyr.csv.processor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;

import com.koldyr.csv.io.DBToFilePipeline;
import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.ProcessorContext;
import com.mysql.cj.api.jdbc.JdbcConnection;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;

/**
 * Description of class PageExportProcessor
 *
 * @created: 2018.03.05
 */
public class PageExportProcessor extends BasePageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PageExportProcessor.class);

    private final DBToFilePipeline dataPipeline;

    public PageExportProcessor(ProcessorContext context, String tableName, DBToFilePipeline dataPipeline) {
        super(tableName, context);
        this.dataPipeline = dataPipeline;
    }

    @Override
    protected void execute(PageBlockData pageBlock) throws SQLException, IOException {
        Thread.currentThread().setName(tableName + '-' + pageBlock.index);

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            long startPage = System.currentTimeMillis();
            LOGGER.debug("Starting {} page  {}", tableName, pageBlock.index);

            final CallWithRetry<Connection> getConnection = new CallWithRetry<>(context::get, 30, 2000, true);
            connection = getConnection.call();
            statement = connection.createStatement();

            String sql = getPageSQL(connection, pageBlock);

            resultSet = statement.executeQuery(sql);

            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount() - 1;  // remove ROWNUM column

            int counter = 0;
            while (dataPipeline.next(resultSet, columnCount)) {
                counter++;

                if (counter % 1000.0 == 0) {
                    dataPipeline.flush();
                    final long percent = Math.round(counter / (double) pageBlock.length * 100.0);
                    LOGGER.debug("\t{}%", percent);
                }
            }

            dataPipeline.flush();

            LOGGER.debug("Finished {} page {} in {} ms", tableName, pageBlock.index, format.format(System.currentTimeMillis() - startPage));
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    context.release(connection);
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private String getPageSQL(Connection connection, PageBlockData pageBlock) {
        boolean oracle = isOracle(connection);
        if (oracle) {
            return "SELECT * FROM (SELECT subQ.*, rownum RNUM FROM ( SELECT * FROM " + context.getSchema() + '.' + tableName +
                    ") subQ WHERE rownum <= " + (pageBlock.start + pageBlock.length) + ") WHERE RNUM > " + pageBlock.start;
        }

        boolean msSQLServer = isMsSQLServer(connection);
        if (msSQLServer) {
            return "SELECT * FROM " + context.getSchema() + '.' + tableName + " OFFSET " + pageBlock.start + " ROWS FETCH NEXT " + pageBlock.length + " ROWS ONLY";
        }

        boolean mySQL = isMySql(connection);
        boolean postgreSQL = isPostgreSQL(connection);
        if (postgreSQL || mySQL) {
            return "SELECT * FROM " + context.getSchema() + '.' + tableName + " LIMIT " + pageBlock.length + " OFFSET " + pageBlock.start;
        }

        return "SELECT * FROM " + context.getSchema() + '.' + tableName;
    }

    private boolean isPostgreSQL(Connection connection) {
        return connection instanceof BaseConnection;
    }

    private boolean isMySql(Connection connection) {
        return connection instanceof JdbcConnection;
    }

    private boolean isMsSQLServer(Connection connection) {
        return connection instanceof ISQLServerConnection;
    }

    private boolean isOracle(Connection connection) {
        return connection instanceof OracleConnection;
    }
}
