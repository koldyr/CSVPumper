package com.koldyr.csv.processor;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description of class RetryCall
 *
 * @created: 2018.03.07
 */
public class RetryCall<V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryCall.class);

    private final Callable<V> command;
    private final Runnable recover;
    private final boolean exponentialBackOff;
    private int maxAttempts;
    private int backOffPeriod;

    public RetryCall(Callable<V> command, Runnable recover, int maxAttempts, int backOffPeriod, boolean exponentialBackOff) {
        this.command = command;
        this.recover = recover;
        this.maxAttempts = maxAttempts;
        this.backOffPeriod = backOffPeriod;
        this.exponentialBackOff = exponentialBackOff;
    }

    public RetryCall(Callable<V> command, int maxAttempts, int backOffPeriod, boolean exponentialBackOff) {
        this(command, null, maxAttempts, backOffPeriod, exponentialBackOff);
    }

    public V call() {
        boolean isSuccess = false;
        int retryCount = 0;
        Throwable throwable = null;
        V result = null;
        while (!isSuccess && maxAttempts > 0) {
            try {
                result = command.call();
                isSuccess = true;
            } catch (Throwable e) {
                throwable = e;
                retryCount++;
                maxAttempts--;

                LOGGER.warn("retry failed. #attempt: {}, sleeping for [ms]: {}", retryCount, backOffPeriod);
                LOGGER.trace(throwable.getMessage(), throwable);

                backOff();
                callRecover();
            }
        }
        if (!isSuccess) {
            LOGGER.warn("Command failed - no recovery possible! #attempt: " + retryCount, throwable);
            throw new RuntimeException(throwable);
        }
        return result;
    }

    private void backOff() {
        try {
            Thread.sleep(backOffPeriod);
        } catch (InterruptedException ee) {
            LOGGER.warn(ee.getMessage());
        }
        if (exponentialBackOff) {
            backOffPeriod = Math.min(backOffPeriod * 2, 60_000);
        }
    }

    private void callRecover() {
        if (recover != null) {
            try {
                recover.run();
            } catch (Throwable ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        }
    }
}
