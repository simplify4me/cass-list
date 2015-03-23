package com.simplify4me.casslist;

import javax.annotation.Nonnull;

/**
 * A policy that drives values read and written from/to the list.
 */
public interface CassListRWPolicy {

    /**
     * Returns an appropriate row key for storing the value. The behavior of this method shall be
     * implementation specific.
     *
     * This method shall not throw an exception and fail.
     *
     * @param listName name of the list
     * @return a row key to store the value against
     */
    String rowKey(@Nonnull String listName);

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
     * @param  listName name of the list
     * @param readerName reader name
     * @return a row key to read or a value of null to indicate nothing to read
     */
    String nextRowToRead(@Nonnull String listName, @Nonnull String readerName);
}
