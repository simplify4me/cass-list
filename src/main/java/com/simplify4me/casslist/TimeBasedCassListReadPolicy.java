package com.simplify4me.casslist;

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
        private final long initialStartTimeMillis;
        private final TimeBasedCassListIndexBuilder indexBuilder;

        private volatile long startFromMillis = 0;

        public LookbackInTimeReadPolicy(TimeBasedCassListIndexBuilder indexBuilder, long startFromMillis) {
            this.indexBuilder = indexBuilder;
            this.initialStartTimeMillis = this.startFromMillis = startFromMillis;
        }

        @Override
        public String nextRowToRead() {
            long now = System.currentTimeMillis();
            if (startFromMillis >= now) { //avoid reading the currently being written or future row
                return null;
            }
            return indexBuilder.build(startFromMillis++);
        }

        @Override
        public void reset() {
            this.startFromMillis = initialStartTimeMillis;
        }

        public static CassListReadPolicy lookback5MinsPolicy(TimeBasedCassListIndexBuilder indexBuilder) {
            final long startFrom = (System.currentTimeMillis() - (5 * 60 * 1000)); //5 mins
            return new LookbackInTimeReadPolicy(indexBuilder, startFrom);
        }
    }
}
