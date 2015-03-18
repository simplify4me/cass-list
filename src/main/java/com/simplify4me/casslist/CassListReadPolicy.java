package com.simplify4me.casslist;

import javax.annotation.Nonnull;

/**
 * A policy that drives which values are read from the list.
 */
public interface CassListReadPolicy {

    /**
     * Returns the next row to read. The behavior of this method shall be
     * implementation specific and 'next' in the method name does not necessarily
     * indicate its incrementally next row/value and/or ordering guarantees of
     * the rows/values read
     *
     * A value of null, when returned to indicate no more values to read, may not indicate
     * a permanent state i.e. since entries can be added to the list in future, this method
     * might return a value AFTER a null return.
     *
     * This method shall not throw an exception and fail.
     *
     * @param consumerName consumer name
     * @return a row key to read or a value of null to indicate nothing to read
     */
    String nextRowToRead(@Nonnull String consumerName);

    /**
     * Resets the state that (@method nextRowToRead) relies on, such that subsequent
     * calls to nextRowToRead can, but not required to, return previously returned values.
     *
     * @throws UnsupportedOperationException if resetting of state is not supported by
     * the implementation
     */
    default void reset() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("reset");
    }
}
