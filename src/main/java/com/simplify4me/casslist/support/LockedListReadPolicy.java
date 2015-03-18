package com.simplify4me.casslist.support;

import javax.annotation.Nonnull;

import com.simplify4me.casslist.CassListReadPolicy;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * A read policy that tries to obtain a 'lock', via Cassandra, globally as a pre-condition
 * for any read. This is useful when you want to limit processing of any given entry to one
 * thread at any point in time.
 *
 * The number of locks defaults to 1 'lock' but it can be configured to a higher value to allow
 * for multiple processes reading entries off of the list. The allocation of keys to locks is done
 * via lock = key.hashCode % numLocks and only processes/threads that own the lock for that partition
 * can process a given key.
 *
 * The lock expires in a pre-defined amount of time, so
 * 1. Lock expiry should be set to be high enough so the entries can be processed within that time
 * 2. Lock expiry should be set low enough to allow another thread to pick it up for processing within
 * the expected SLA
 *
 */
public class LockedListReadPolicy implements CassListReadPolicy {

    private final String listName;
    private final CassLock cassLock;
    private final CassListReadPolicy delegatePolicy;

    private int numLocks = 1;

    public LockedListReadPolicy(@Nonnull CassListCF cassListCF,
                                @Nonnull CassListReadPolicy readPolicy,
                                @Nonnull String listName) {

        this.delegatePolicy = readPolicy;
        this.listName = listName;

        this.cassLock = new CassLock(cassListCF, Integer.valueOf(300)); //5 mins
    }

    public void setNumLocks(int numLocks) {
        if (numLocks < 1) throw new IllegalStateException();
        this.numLocks = numLocks;
    }

    @Override
    public String nextRowToRead(@Nonnull String consumerName) {
        try {
            final String rowToRead = delegatePolicy.nextRowToRead(consumerName);
            if (rowToRead != null) {
                final int bucket = rowToRead.hashCode() % numLocks;
                if (cassLock.tryLock("L:" + listName + ":C:" + consumerName + ":B:" + bucket)) {
                    return rowToRead;
                }
            }
        }
        catch (ConnectionException e) {
            e.printStackTrace();
        }
        return null;
    }
}
