package com.simplify4me.casslist;

import java.util.UUID;

import com.simplify4me.casslist.support.TimeBasedCassListIndexBuilder;

/**
 * A policy that drives which values are read from the list, based on a time.
 */
public interface TimeBasedCassListReadPolicy extends CassListReadPolicy {

    /**
     * Resets the state that (@method nextRowToRead) relies on, such that subsequent
     * calls to nextRowToRead can, but not required to, return previously returned values.
     *
     * @throws UnsupportedOperationException if resetting of state is not supported by
     * the implementation
     */
    void reset() throws UnsupportedOperationException;

    class LookbackInTimeReadPolicy implements TimeBasedCassListReadPolicy {
        private final long initialValue;
        private final TimeBasedCassListIndexBuilder indexBuilder;

        private volatile long time = 0;
        private volatile long startFromSec = 0;

        public LookbackInTimeReadPolicy(TimeBasedCassListIndexBuilder indexBuilder, long startFromSec) {
            this.indexBuilder = indexBuilder;
            this.initialValue = this.startFromSec = startFromSec;
            this.time = now();
        }

        @Override
        public String nextRowToRead() {
            if (startFromSec > (time-1)) {
                time = now();
                if (startFromSec > (time-1)) return null;
            }
            return indexBuilder.build(startFromSec++);
        }

        private static long now() {
            return System.currentTimeMillis()/1000;
        }

        @Override
        public void reset() {
            this.startFromSec = initialValue;
        }

        public static CassListReadPolicy lookback5MinsPolicy(TimeBasedCassListIndexBuilder indexBuilder) {
            final long startFromSec = (now()) - 300; //5 mins
            return new LookbackInTimeReadPolicy(indexBuilder, startFromSec);
        }
    }
}
