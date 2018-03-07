package com.koldyr.csv.processor;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.concurrent.Callable;

import com.koldyr.csv.model.ProcessorContext;

/**
 * Description of class DBProcessor
 *
 * @created: 2018.03.03
 */
public abstract class BatchDBProcessor implements Callable<Object> {

    protected final DecimalFormat format;
    protected final ProcessorContext context;

    protected BatchDBProcessor(ProcessorContext context) {
        this.context = context;

        final DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setGroupingSeparator(',');
        format = new DecimalFormat("###,###,###,###", decimalFormatSymbols);
    }

    @Override
    public Object call() {
        String tableName = context.getNextTable();
        while (tableName != null) {
            processTable(tableName);
            tableName = context.getNextTable();
        }

        return null;
    }

    protected abstract void processTable(String tableName);
}
