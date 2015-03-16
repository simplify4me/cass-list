package com.simplify4me.casslist;

/**
 * A builder to generate indices for entries in the list. The indices, in this case, represent
 * the cassandra row-key for the corresponding entries
 *
 */
public interface CassListIndexBuilder {
    String build();
}
