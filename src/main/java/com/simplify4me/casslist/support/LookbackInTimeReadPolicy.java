package com.simplify4me.casslist.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

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

    private final Map<String, AtomicLong> timeMap = new ConcurrentHashMap<>();

    public LookbackInTimeReadPolicy(TimeBasedCassListIndexBuilder indexBuilder, long startFromSecs) {
        this.indexBuilder = indexBuilder;
        this.initialStartTimeSecs = startFromSecs;
    }

    @Override
    public String nextRowToRead(@Nonnull String consumerName) {
        long now = System.currentTimeMillis()/1000;
        AtomicLong time = timeMap.computeIfAbsent(consumerName, f -> new AtomicLong(initialStartTimeSecs));
        if (time.longValue() >= now) { //avoid reading the currently being written or future row
            return null;
        }
        return indexBuilder.build(time.getAndIncrement());
    }

    @Override
    public void reset() {
        timeMap.clear();
    }

    public static CassListReadPolicy lookback5MinsPolicy(TimeBasedCassListIndexBuilder indexBuilder) {
        final long startFrom = (System.currentTimeMillis() - (5 * 60 * 1000)); //5 mins
        return new com.simplify4me.casslist.support.LookbackInTimeReadPolicy(indexBuilder, startFrom/1000);
    }
}
