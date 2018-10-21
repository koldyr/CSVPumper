package com.koldyr.csv.processor;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.concurrent.Callable;

import org.slf4j.LoggerFactory;

import com.koldyr.csv.model.PageBlockData;
import com.koldyr.csv.model.ProcessorContext;

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

        PageBlockData pageBlock;
        while ((pageBlock = context.getNextPageBlock(tableName)) != null) {
            try {
                execute(pageBlock);

                processedBlocks++;
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).error(e.getMessage(), e);
                return processedBlocks;
            }
        }

        return processedBlocks;
    }

    protected abstract void execute(PageBlockData pageBlock) throws SQLException, IOException;
}
