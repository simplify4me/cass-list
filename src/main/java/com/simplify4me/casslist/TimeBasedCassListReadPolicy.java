package com.simplify4me.casslist;

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

    /**
     * Returns the next time 'slot' to read. The behavior of this method shall be
     * implementation specific and 'next' in the method name does not necessarily
     * indicate an incremental next value and no ordering guarantees are made for
     * the values returned
     *
     * This method shall not throw an exception and fail.
     *
     * @return the next time or a value of 0 to indicate nothing to read
     */
    long nextRowToRead();

    class LookbackInTimeReadPolicy implements TimeBasedCassListReadPolicy {
        private final long initialValue;
        private volatile long time = 0;
        private volatile long startFromSec = 0;

        public LookbackInTimeReadPolicy(long startFromSec) {
            this.initialValue = this.startFromSec = startFromSec;
            this.time = now();
        }

        @Override
        public long nextRowToRead() {
            if (startFromSec > (time-1)) {
                time = now();
                if (startFromSec > (time-1)) return 0;
            }
            return startFromSec++;
        }

        private static long now() {
            return System.currentTimeMillis()/1000;
        }

        @Override
        public void reset() {
            this.startFromSec = initialValue;
        }

        public static CassListReadPolicy lookback5MinsPolicy() {
            final long startFromSec = (now()) - 300; //5 mins
            return new LookbackInTimeReadPolicy(startFromSec);
        }
    }
}
