package com.koldyr.csv;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.koldyr.csv.db.ConnectionsFactory;
import com.koldyr.csv.model.ConnectionData;
import com.koldyr.csv.model.Operation;
import com.koldyr.csv.model.PoolType;
import com.koldyr.csv.model.ProcessorContext;
import com.koldyr.csv.processor.copy.CopyProcessor;
import com.koldyr.csv.processor.export.ExportProcessor;
import com.koldyr.csv.processor.imprt.ImportProcessor;

/**
 * Description of class CSVExport
 *
 * @created: 2018.03.02
 */
public class CSVBatchProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVBatchProcessor.class);

    public static void main(String[] args) throws IOException {
        Properties dbConfig = new Properties();

        try (FileInputStream inputStream = new FileInputStream("db-config.properties")) {
            dbConfig.load(inputStream);
        }

        final Operation operation = Operation.valueOf(dbConfig.getProperty("operation"));
        final ConnectionData srcConfig = getDBConfig(dbConfig, "source");
        final ConnectionData dstConfig = getDBConfig(dbConfig, "destination");
        final String path = dbConfig.getProperty("path");

        final KeyedObjectPool<PoolType, Connection> connectionsPool = createConnectionsPool(srcConfig, dstConfig);

        LOGGER.debug("Load table names...");
        final List<String> tableNames = loadTableNames();

        loadProcessConfig(connectionsPool);

        final int threadCount = Math.min(Constants.PARALLEL_TABLES, tableNames.size());
        final ExecutorService executor = Executors.newCachedThreadPool();

        try {
            LOGGER.debug("Create processors");

            ProcessorContext context = new ProcessorContext(connectionsPool, tableNames);
            context.setPath(path);
            context.setSrcSchema(srcConfig.getSchema());
            context.setDstSchema(dstConfig.getSchema());
            context.setExecutor(executor);
            context.setPageSize(Constants.PAGE_SIZE);

            Collection<Callable<Object>> processors = new LinkedList<>();
            for (int i = 0; i < threadCount; i++) {
                processors.add(createProcessor(operation, context));
            }

            LOGGER.debug("Start...");
            executor.invokeAll(processors);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            connectionsPool.close();
            executor.shutdown();
        }
    }

    private static ConnectionData getDBConfig(Properties params, String prefix) {
        return new ConnectionData(params.getProperty(prefix + ".url"),
                params.getProperty(prefix + ".schema"),
                params.getProperty(prefix + ".user"),
                params.getProperty(prefix + ".password"));
    }

    private static void loadProcessConfig(KeyedObjectPool<PoolType, Connection> connectionsPool) {
        try {
            Properties config = new Properties();
            config.load(new FileInputStream("process-config.properties"));
            Constants.MAX_CONNECTIONS = Integer.parseInt(config.getProperty("max-connections"));
            Constants.PAGE_SIZE = Long.parseLong(config.getProperty("page-size"));
            Constants.PARALLEL_TABLES = Integer.parseInt(config.getProperty("parallel-tables"));
            Constants.PARALLEL_PAGES = Integer.parseInt(config.getProperty("parallel-pages"));

            int srcMaxConnections = getMaxConnections(connectionsPool, PoolType.SOURCE);
            int dstMaxConnections = getMaxConnections(connectionsPool, PoolType.DESTINATION);

            List<Integer> mxCons = Arrays.asList(Constants.MAX_CONNECTIONS, srcMaxConnections, dstMaxConnections);
            Constants.MAX_CONNECTIONS = Collections.min(mxCons);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static int getMaxConnections(KeyedObjectPool<PoolType, Connection> connectionsPool, PoolType type) throws Exception {
        int result;
        Connection connection = null;
        try {
            connection = connectionsPool.borrowObject(type);
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            result = databaseMetaData.getMaxConnections();
            if (result == 0) {
                result = Integer.MAX_VALUE;
            }
        } catch (Exception e) {
            result = Integer.MAX_VALUE;
        } finally {
            if (connection != null) {
                connectionsPool.returnObject(type, connection);
            }
        }
        return result;
    }

    private static KeyedObjectPool<PoolType, Connection> createConnectionsPool(ConnectionData srcConfig, ConnectionData dstConfig) {
        final ConnectionsFactory factory = new ConnectionsFactory(srcConfig, dstConfig);
        final GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setMaxTotalPerKey(Constants.MAX_CONNECTIONS);
        config.setMaxIdlePerKey(1);
        config.setMinEvictableIdleTimeMillis(100);
        config.setNumTestsPerEvictionRun(100);
        config.setTimeBetweenEvictionRunsMillis(1000);
        return new GenericKeyedObjectPool<>(factory, config);
    }

    private static Callable<Object> createProcessor(Operation operation, ProcessorContext context) {
        switch (operation) {
            case EXPORT:
                return new ExportProcessor(context);
            case IMPORT:
                return new ImportProcessor(context);
            case COPY:
                return new CopyProcessor(context);
        }

        throw new IllegalArgumentException("Unsupported operation " + operation);
    }

    private static List<String> loadTableNames() {
        File tables = new File("tables.config");
        if (!tables.exists()) {
            LOGGER.warn("No tables.config file found");
            try {
                tables.createNewFile();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            return Collections.emptyList();
        }

        try (LineIterator lines = IOUtils.lineIterator(new FileInputStream(tables), UTF_8)) {
            List<String> result = new LinkedList<>();

            while (lines.hasNext()) {
                final String line = lines.next();
                if (!line.startsWith("#")) {
                    result.add(line);
                }
            }
            return result;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }

        return Collections.emptyList();
    }
}
