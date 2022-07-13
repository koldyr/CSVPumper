package com.koldyr.util

import mu.KotlinLogging
import java.time.Duration

/**
 * Description of the Timing class
 *
 * @author d.halitski@gmail.com
 * @created 2022-07-13
 */

inline fun <T> executeWithTimer(msg: String, call: () -> T): T {
    return executeWithTimer(msg, null, call)
}

inline fun <T> executeWithTimer(msg: String, noinline exceptionHandler: ((ex: Exception) -> T)? = null, call: () -> T): T {
    val log = KotlinLogging.logger { }
    val start = System.currentTimeMillis()

    return try {
        log.debug { "STARTED: $msg" }
        return call()
    } catch (e: Exception) {
        if (exceptionHandler == null) throw e

        log.error(e) { "FAILED: $msg" }
        exceptionHandler(e)
    } finally {
        val seconds = Duration.ofMillis(System.currentTimeMillis() - start).toSeconds()
        log.debug { "FINISHED: $msg in $seconds" }
    }
}
