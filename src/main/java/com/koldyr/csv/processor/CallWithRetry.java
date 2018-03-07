package com.koldyr.csv.processor;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description of class CallWithRetry
 *
 * @created: 2018.03.07
 */
public class CallWithRetry<V> {
    private static final Logger log = LoggerFactory.getLogger(CallWithRetry.class.getName());

    private final Callable<V> cmd;
    private final Runnable cmdAfterFailure;
    private int maxAttempts;
    private int backOffPeriod;
    private final boolean exponentialBackoff;

    public CallWithRetry(Callable<V> cmd, Runnable cmdAfterFailure, int maxAttempts, int backOffPeriod, boolean exponentialBackoff) {
        this.cmd = cmd;
        this.cmdAfterFailure = cmdAfterFailure;
        this.maxAttempts = maxAttempts;
        this.backOffPeriod = backOffPeriod;
        this.exponentialBackoff = exponentialBackoff;
    }

    public CallWithRetry(Callable<V> cmd, int maxAttempts, int backOffPeriod, boolean exponentialBackoff) {
        this(cmd, null, maxAttempts, backOffPeriod, exponentialBackoff);
    }

    public V call() {
        boolean isSuccess = false;
        int retryCount = 0;
        Throwable t = null;
        V result = null;
        while (!isSuccess && maxAttempts > 0) {
            try {
                result = cmd.call();
                isSuccess = true;
            } catch (Throwable e) {
                t = e;
                retryCount++;
                maxAttempts--;
                log.warn("retry failed. #attempt: " + retryCount + " sleeping for [ms] :" + backOffPeriod);
                log.trace("", t);
                try {
                    Thread.sleep(backOffPeriod);
                } catch (InterruptedException ee) {
                    log.warn(ee.getMessage());
                }
                if (cmdAfterFailure != null) {
                    try {
                        cmdAfterFailure.run();
                    } catch (Throwable e1) {
                        log.error(e1.getMessage(), e1);
                    }
                }
                if (exponentialBackoff) {
                    backOffPeriod = Math.min(backOffPeriod * 2, 60_000);
                }
            }
        }
        if (!isSuccess) {
            log.warn("Command failed - no recovery possible! #attempt: " + retryCount, t);
            throw new RuntimeException(t);
        }
        return result;
    }

}
