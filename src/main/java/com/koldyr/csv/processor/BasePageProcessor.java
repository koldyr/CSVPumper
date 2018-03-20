/*
 * (c) 2012-2018 Swiss Re. All rights reserved.
 */
package com.koldyr.csv.processor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.concurrent.Callable;

import org.postgresql.core.BaseConnection;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;

import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.ProcessorContext;
import com.mysql.cj.api.jdbc.JdbcConnection;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;

/**
 * Description of class BasePageProcessor
 *
 * @created: 2018.03.10
 */
public abstract class BasePageProcessor implements Callable<Integer> {
    protected final ProcessorContext context;
    protected final String tableName;
    protected final DecimalFormat format;

    protected BasePageProcessor(String tableName, ProcessorContext context) {
        this.tableName = tableName;
        this.context = context;

        final DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setGroupingSeparator(',');
        format = new DecimalFormat("###,###,###,###", decimalFormatSymbols);
    }

    @Override
    public Integer call() {
        int processedBlocks = 0;

        PageBlockData pageBlock = context.getNextPageBlock(tableName);
        while (pageBlock != null) {
            try {
                execute(pageBlock);

                processedBlocks++;
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).error(e.getMessage(), e);
                return processedBlocks;
            }

            pageBlock = context.getNextPageBlock(tableName);
        }

        return processedBlocks;
    }

    protected abstract void execute(PageBlockData pageBlock) throws SQLException, IOException;

    protected String getPageSQL(Connection connection, PageBlockData pageBlock) {
        boolean oracle = isOracle(connection);
        if (oracle) {
            return "SELECT * FROM (SELECT subQ.*, rownum RNUM FROM ( SELECT * FROM " + context.getSrcSchema() + '.' + tableName +
                    " ORDER BY 1) subQ WHERE rownum <= " + (pageBlock.start + pageBlock.length) + " ORDER BY 1) WHERE RNUM > " + pageBlock.start + " ORDER BY 1";
        }

        boolean msSQLServer = isMsSQLServer(connection);
        if (msSQLServer) {
            return "SELECT * FROM " + context.getSrcSchema() + '.' + tableName + " ORDER BY 1 OFFSET " + pageBlock.start + " ROWS FETCH NEXT " + pageBlock.length + " ROWS ONLY";
        }

        boolean mySQL = isMySql(connection);
        boolean postgreSQL = isPostgreSQL(connection);
        if (postgreSQL || mySQL) {
            return "SELECT * FROM " + context.getSrcSchema() + '.' + tableName + " ORDER BY 1 LIMIT " + pageBlock.length + " OFFSET " + pageBlock.start;
        }

        return "SELECT * FROM " + context.getSrcSchema() + '.' + tableName;
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
