package com.simplify4me.casslist.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.log4j.Logger;

import com.simplify4me.casslist.CassListRWPolicy;

/**
 * A policy that drives which values are read from the list, based on a time.
 *
 * Policy to control reads based on time (starting from the past). It restricts reads to
 * past time, no current or future time based reads.
 */
public class TimeBasedRWPolicy implements CassListRWPolicy {

    private static final Logger logger = Logger.getLogger(TimeBasedRWPolicy.class);

    private volatile long startTime = 0;
    private volatile long initialStartTimeDiffSecs = 0;

    private TimeBasedRWPolicy timeObj = null;

    private final Map<String, AtomicLong> timeMap = new ConcurrentHashMap<>();

    public TimeBasedRWPolicy(long startFromSecs) {
        this.startTime = startFromSecs;
        this.initialStartTimeDiffSecs = TimeInSec.now() - startFromSecs;
    }

    public TimeBasedRWPolicy(long startFromSecs, TimeBasedRWPolicy timeObj) {
        this(startFromSecs);
        this.timeObj = timeObj;
    }

    @Override
    public String rowKey(@Nonnull String listName) {
        return listName + "." + TimeInSec.now();
    }

    @Override
    public String nextRowToRead(@Nonnull String listName, @Nonnull String readerName) {
        AtomicLong time = timeMap.computeIfAbsent(readerName, f -> new AtomicLong(this.startTime));
        return listName + "." + next(time);
    }

    public long next(AtomicLong time) {
        final long end = (timeObj == null ? TimeInSec.now() : timeObj.startTime);
        if (time.longValue() >= end) {
            time.set(TimeInSec.minusSecs(this.initialStartTimeDiffSecs));
        }
        return time.getAndIncrement();
    }
}
