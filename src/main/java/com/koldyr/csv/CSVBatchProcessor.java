package com.koldyr.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.koldyr.csv.db.ConnectionsFactory;
import com.koldyr.csv.model.ConnectionData;
import com.koldyr.csv.model.ProcessorContext;
import com.koldyr.csv.processor.ExportProcessor;
import com.koldyr.csv.processor.ImportProcessor;

/**
 * Description of class CSVExport
 *
 * @created: 2018.03.02
 */
public class CSVBatchProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVBatchProcessor.class);

    public static void main(String[] args) {
        String url = args[0];
        String user = args[1];
        String password = args[2];
        String operation = args.length > 3 ? args[3] : "export";
        String schema = args.length > 4 ? args[4] : "GEO";
        String path = args.length > 5 ? args[5] : ".";

        LOGGER.debug("Load table names...");
        final List<String> tableNames = loadTableNames();

        final int threadCount = Math.min(Constants.PARALLEL_TABLES, tableNames.size());
        final ExecutorService executor = Executors.newCachedThreadPool();

        final ConnectionsFactory factory = new ConnectionsFactory(new ConnectionData(url, user, password));
        final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(1);
        config.setMinEvictableIdleTimeMillis(100);
        config.setNumTestsPerEvictionRun(100);
        config.setTimeBetweenEvictionRunsMillis(1000);
        ObjectPool<Connection> connectionsPool = new GenericObjectPool<>(factory, config);

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

    private static Callable<Object> createProcessor(String operation, ProcessorContext commonConfig) {
        if (operation.equals("export")) {
            return new ExportProcessor(commonConfig);
        }
        return new ImportProcessor(commonConfig);
    }

    private static List<String> loadTableNames() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(CSVBatchProcessor.class.getResourceAsStream("/tables.txt")))) {
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
