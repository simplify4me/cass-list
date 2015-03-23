package com.simplify4me.casslist.support;

import javax.annotation.Nonnull;

import com.simplify4me.casslist.CassListRWPolicy;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * A read policy that tries to obtain a 'lock', via Cassandra, globally as a pre-condition
 * for any read. This is useful when you want to limit processing of any given entry to one
 * thread at any point in time.
 *
 * The lock expires in a pre-defined amount of time, so
 * 1. Lock expiry should be set to be high enough so the entries can be processed within that time
 * 2. Lock expiry should be set low enough to allow another thread to pick it up for processing within
 * the expected SLA
 *
 */
public class LockedListRWPolicy implements CassListRWPolicy {

    private final CassLock cassLock;
    private final CassListRWPolicy delegatePolicy;

    public LockedListRWPolicy(@Nonnull CassLock cassLock,
                              @Nonnull CassListRWPolicy delegate) {

        this.delegatePolicy = delegate;
        this.cassLock = cassLock;
    }

    @Override
    public String rowKey(@Nonnull String listName) {
        return delegatePolicy.rowKey(listName);
    }

    @Override
    public String nextRowToRead(@Nonnull String listName, @Nonnull String readerName) {
        try {
            if (cassLock.tryLock("L:" + listName + ":C:" + readerName)) {
                return delegatePolicy.nextRowToRead(listName, readerName);
            }
        }
        catch (ConnectionException e) {
            e.printStackTrace();
        }
        return null;
    }
}
