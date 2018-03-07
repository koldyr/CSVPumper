package com.koldyr.csv.model;

/**
 * Description of class PageBlockData
 *
 * @created: 2018.03.06
 */
public class PageBlockData {
    public final int index;
    public final long start;
    public final long length;

    public PageBlockData(int index, long start, long length) {
        this.index = index;
        this.start = start;
        this.length = length;
    }
}
