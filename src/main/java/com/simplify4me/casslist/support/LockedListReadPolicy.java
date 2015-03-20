package com.simplify4me.casslist.support;

import javax.annotation.Nonnull;

import com.simplify4me.casslist.CassListReadPolicy;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * A read policy that tries to obtain a 'lock', via Cassandra, globally as a pre-condition
 * for any read. This is useful when you want to limit processing of any given entry to one
 * thread at any point in time.
 *
 * The number of concurrent readers defaults to 1 'lock' but it can be configured to a higher value to allow
 * for concurrent reading of entries from the list. The allocation of keys to locks is done
 * via lock = key.hashCode % numConcurrentReaders and only threads that own the lock for that partition
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

    private int numConcurrentReaders = 1;

    public LockedListReadPolicy(@Nonnull CassListCF cassListCF,
                                @Nonnull CassListReadPolicy readPolicy,
                                @Nonnull String listName) {

        this.delegatePolicy = readPolicy;
        this.listName = listName;

        this.cassLock = new CassLock(cassListCF, Integer.valueOf(300)); //5 mins
    }

    public void setNumConcurrentReaders(int numConcurrentReaders) {
        if (numConcurrentReaders < 1) throw new IllegalStateException();
        this.numConcurrentReaders = numConcurrentReaders;
    }

    @Override
    public String nextRowToRead(@Nonnull String readerName) {
        try {
            final String rowToRead = delegatePolicy.nextRowToRead(readerName);
            if (rowToRead != null) {
                final int bucket = rowToRead.hashCode() % numConcurrentReaders;
                if (cassLock.tryLock("L:" + listName + ":C:" + readerName + ":B:" + bucket)) {
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
