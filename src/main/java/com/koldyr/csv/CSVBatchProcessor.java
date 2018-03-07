package com.koldyr.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        final int threadCount = Math.min(10, tableNames.size());
        final ExecutorService executor = Executors.newCachedThreadPool();

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setMaxTotal(100);
        dataSource.setTimeBetweenEvictionRunsMillis(5000);
        dataSource.setMinEvictableIdleTimeMillis(1000 * 60);
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);

        try {
            LOGGER.debug("Create processors");

            ProcessorContext context = new ProcessorContext(dataSource, tableNames);
            context.setPath(path);
            context.setSchema(schema);
            context.setExecutor(executor);
            context.setPageSize(400_000);

            Collection<Callable<Object>> processors = new LinkedList<>();
            for (int i = 0; i < threadCount; i++) {
                processors.add(createProcessor(operation, context));
            }

            executor.invokeAll(processors);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            try {
                dataSource.close();
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
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

        return new Stack<>();
    }
}
