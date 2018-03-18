package com.koldyr.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Properties params = new Properties();

        try (FileInputStream inputStream = new FileInputStream("params.properties")) {
            params.load(inputStream);
        }

        final Operation operation = Operation.valueOf(params.getProperty("operation"));
        final ConnectionData srcConfig = getDBConfig(params, "source");
        final ConnectionData dstConfig = getDBConfig(params, "destination");
        final String schema = params.getProperty("schema");
        final String path = params.getProperty("path");

        LOGGER.debug("Load table names...");
        final List<String> tableNames = loadTableNames();

        loadConfig();

        final int threadCount = Math.min(Constants.PARALLEL_TABLES, tableNames.size());
        final ExecutorService executor = Executors.newCachedThreadPool();

        KeyedObjectPool<PoolType, Connection> connectionsPool = createConnectionsPool(srcConfig, dstConfig);

        try {
            LOGGER.debug("Create processors");

            ProcessorContext context = new ProcessorContext(connectionsPool, tableNames);
            context.setPath(path);
            context.setSchema(schema);
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
                params.getProperty(prefix + ".user"),
                params.getProperty(prefix + ".password"));
    }

    private static void loadConfig() {
        try {
            Properties config = new Properties();
            config.load(new FileInputStream("config.properties"));
            Constants.MAX_CONNECTIONS = Integer.parseInt(config.getProperty("max-connections"));
            Constants.PAGE_SIZE = Long.parseLong(config.getProperty("page-size"));
            Constants.PARALLEL_TABLES = Integer.parseInt(config.getProperty("parallel-tables"));
            Constants.PARALLEL_PAGES = Integer.parseInt(config.getProperty("parallel-pages"));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
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

    private static Callable<Object> createProcessor(Operation operation, ProcessorContext commonConfig) {
        switch (operation) {
            case EXPORT:
                return new ExportProcessor(commonConfig);
            case IMPORT:
                return new ImportProcessor(commonConfig);
            case COPY:
                return new CopyProcessor(commonConfig);
        }

        throw new IllegalArgumentException("Unsupported operation " + operation);
    }

    private static List<String> loadTableNames() {
        File tables = new File("tables.txt");
        if (!tables.exists()) {
            LOGGER.warn("No tables.txt file found");
            try {
                tables.createNewFile();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            return Collections.emptyList();
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(tables)))) {
            List<String> result = new LinkedList<>();

            String line = reader.readLine();
            while (line != null) {
                if (!line.startsWith("#")) {
                    result.add(line);
                }
                line = reader.readLine();
            }
            return result;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }

        return Collections.emptyList();
    }
}
