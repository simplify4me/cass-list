package com.simplify4me.casslist;

import javax.annotation.Nonnull;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * A list implementation with entries stored in Cassandra for short-term persistence.
 *
 * An standard messaging/queueing (for e.g. JMS based, SQS, Kafka etc) would likely be a best
 * solution for most use cases where cross data center hand-off isn't involved.
 *
 * This works best in scenarios where there is a hand-off involved between processes that are
 * deployed across data centers, especially when
 *
 * 1. The data needed by the process to execute the hand-off is in Cassandra
 * 2. The hand-off is expected to be processed by one or more remote data centers
 *
 * Its simpler to take advantage of cassandra replication, especially when the source data is
 * already in Cassandra
 */
public interface CassList {

    /**
     * Time after which the (key-value) expires and isn't available to read
     *
     * @param entryExpiryInSecs TTL for entries in the list
     */
    void setEntryExpiryInSecs(int entryExpiryInSecs);

    /**
     * Add an entry to the list
     *
     * @param value value
     * @return rowKey for row where the (key-value) were stored
     * @throws ConnectionException
     */
    String add(@Nonnull String value) throws ConnectionException;

    /**
     * Add an entry to the list using a mutation batch that's already part of your write. Handy
     * for batching writes.
     *
     * @param value value
     * @return rowKey for row where the (key-value) were stored
     * @throws ConnectionException
     */
    String add(@Nonnull MutationBatch mutationBatch, @Nonnull String value) throws ConnectionException;

    /**
     * Read a set of entries
     *
     * @param consumerName reading consumer name
     * @return entries read or null, if none available
     * @throws ConnectionException
     */
    CassListEntries read(@Nonnull String consumerName) throws ConnectionException;

    /**
     * Mark the set of entries as read to avoid the same entries being returned by read, if desired.
     * This is an optional operation, refer to implementation for details.
     *
     * @param entry entry to be marked as read
     * @throws ConnectionException
     * @throws java.lang.UnsupportedOperationException
     */
    default void markAsRead(@Nonnull CassListEntries entry) throws UnsupportedOperationException, ConnectionException {
        throw new UnsupportedOperationException("markAsRead");
    }
}
