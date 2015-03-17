package com.simplify4me.casslist.support;

import com.simplify4me.casslist.CassListReadPolicy;
import com.simplify4me.casslist.TimeBasedCassListIndexBuilder;

/**
 * A policy that drives which values are read from the list, based on a time.
 *
 * Policy to control reads based on time (starting from the past). It restricts reads to
 * past time, no current or future time based reads.
 */
public class LookbackInTimeReadPolicy implements CassListReadPolicy {

    private final long initialStartTimeSecs;
    private final TimeBasedCassListIndexBuilder indexBuilder;

    private volatile long startFromSecs = 0;

    public LookbackInTimeReadPolicy(TimeBasedCassListIndexBuilder indexBuilder, long startFromSecs) {
        this.indexBuilder = indexBuilder;
        this.initialStartTimeSecs = this.startFromSecs = startFromSecs;
    }

    @Override
    public String nextRowToRead() {
        long now = System.currentTimeMillis()/1000;
        if (startFromSecs >= now) { //avoid reading the currently being written or future row
            return null;
        }
        return indexBuilder.build(startFromSecs++);
    }

    @Override
    public void reset() {
        this.startFromSecs = initialStartTimeSecs;
    }

    public static CassListReadPolicy lookback5MinsPolicy(TimeBasedCassListIndexBuilder indexBuilder) {
        final long startFrom = (System.currentTimeMillis() - (5 * 60 * 1000)); //5 mins
        return new com.simplify4me.casslist.support.LookbackInTimeReadPolicy(indexBuilder, startFrom/1000);
    }
}
