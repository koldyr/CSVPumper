package com.koldyr.csv

/**
 * Description of class Constants
 *
 * @created: 2018.03.08
 */
object Constants {
    var MAX_CONNECTIONS = 100
    var PAGE_SIZE: Long = 100000
    var PARALLEL_TABLES = 5
    var PARALLEL_PAGES = 40

    /**
     * Oracle fetch size is 10 by default which is a performance killer for large result sets.
     */
    var FETCH_SIZE = 50000
}
