package com.koldyr.csv.processor

import java.util.concurrent.Callable
import kotlin.math.min
import org.slf4j.LoggerFactory

/**
 * Description of class RetryCall
 *
 * @created: 2018.03.07
 */
class RetryCall<V>(
    private val command: Callable<V>,
    private val recover: Runnable?,
    private var maxAttempts: Int,
    private var backOffPeriod: Int,
    private val exponentialBackOff: Boolean
) {

    constructor(command: Callable<V>, maxAttempts: Int, backOffPeriod: Int, exponentialBackOff: Boolean) : this(command, null, maxAttempts, backOffPeriod, exponentialBackOff)

    fun call(): V? {
        var isSuccess = false
        var retryCount = 0
        var throwable: Throwable? = null
        var result: V? = null
        while (!isSuccess && maxAttempts > 0) {
            try {
                result = command.call()
                isSuccess = true
            } catch (e: Throwable) {
                throwable = e
                retryCount++
                maxAttempts--

                LOGGER.warn("Retry failed. #attempt: {}, sleeping for [ms]: {}", retryCount, backOffPeriod)
                LOGGER.trace(throwable.message, throwable)

                backOff()
                callRecover()
            }

        }
        if (!isSuccess) {
            LOGGER.warn("Command failed - no recovery possible! #attempt: $retryCount", throwable)
            throw RuntimeException(throwable)
        }
        return result
    }

    private fun backOff() {
        try {
            Thread.sleep(backOffPeriod.toLong())
        } catch (ee: InterruptedException) {
            LOGGER.warn(ee.message)
        }

        if (exponentialBackOff) {
            backOffPeriod = min(backOffPeriod * 2, 60000)
        }
    }

    private fun callRecover() {
        if (recover != null) {
            try {
                recover.run()
            } catch (ex: Throwable) {
                LOGGER.error(ex.message, ex)
            }

        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(RetryCall::class.java)
    }
}
