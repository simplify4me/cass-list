package com.simplify4me.casslist.support;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.log4j.Logger;

import com.simplify4me.casslist.CassListReadPolicy;
import com.simplify4me.casslist.TimeBasedCassListIndexBuilder;

/**
 * A policy that drives which values are read from the list, based on a time.
 *
 * Policy to control reads based on time (starting from the past). It restricts reads to
 * past time, no current or future time based reads.
 */
public class LookbackInTimeReadPolicy implements CassListReadPolicy {

    private static final Logger logger = Logger.getLogger(LookbackInTimeReadPolicy.class);

    private long initialStartTimeSecs = 0;
    private final long initialStartTimeDiffSecs;
    private final TimeBasedCassListIndexBuilder indexBuilder;

    private final Map<String, AtomicLong> timeMap = new ConcurrentHashMap<>();

    public LookbackInTimeReadPolicy(TimeBasedCassListIndexBuilder indexBuilder, long startFromSecs) {
        this.indexBuilder = indexBuilder;
        this.initialStartTimeSecs = startFromSecs;
        this.initialStartTimeDiffSecs = TimeInSec.now() - startFromSecs;
    }

    @Override
    public String nextRowToRead(@Nonnull String readerName) {
        AtomicLong time = timeMap.computeIfAbsent(readerName, f -> new AtomicLong(initialStartTimeSecs));
        if (time.longValue() >= TimeInSec.now()) { //avoid reading the currently being written or future row
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("next-row-read=" + time.get() + ";" + new Date(time.get() * 1000));
        }
        return indexBuilder.build(time.getAndIncrement());
    }

    @Override
    public void reset() {
        this.initialStartTimeSecs = TimeInSec.minusSecs(this.initialStartTimeDiffSecs);
        timeMap.clear();
    }
}
