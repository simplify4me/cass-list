# cass-list
Cassandra based list (instead of a queue)

A list implementation with entries stored in Cassandra for short-term persistence.

An standard messaging/queueing (for e.g. JMS based, SQS, Kafka etc) would likely be a best
solution for most use cases where cross data center hand-off isn't involved.

This works best in scenarios where there is a hand-off involved between processes that are
deployed across data centers, especially when

1. The data needed by the process to execute the hand-off is in Cassandra
2. The hand-off is expected to be processed by one or more remote data centers

Its simpler to take advantage of cassandra replication, especially when the source data is
already in Cassandra

This implementation tries to limit/avoid the death by tombstone problem (https://issues.apache.org/jira/browse/CASSANDRA-6117)
that's seems to be common across queue-like implementations on top of Cassandra by not reusing any of the rows.

List read's are scoped to named readers, which allows for multiple readers to read the
same entries in the list and track entries read per reader.

See com.simplify4me.casslist.CassListTest for usage
