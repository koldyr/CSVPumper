package com.koldyr.csv;

/**
 * Description of class Constants
 *
 * @created: 2018.03.08
 */
public class Constants {
    public static int MAX_CONNECTIONS = 100;
    public static long PAGE_SIZE = 100_000;
    public static int PARALLEL_TABLES = 5;
    public static int PARALLEL_PAGES = 40;

    /**
     * Oracle fetch size is 10 by default which is a performance killer for large result sets.
     */
    public static int FETCH_SIZE = 50000;
}
