package com.simplify4me.casslist;

/**
 *
 */
public final class TimeBasedCassListIndexBuilder implements CassListIndexBuilder {
    private final String listName;

    public TimeBasedCassListIndexBuilder() {
        this("default");
    }

    public TimeBasedCassListIndexBuilder(String listName) {
        this.listName = listName;
    }

    @Override
    public String build() {
        return build(System.currentTimeMillis()/1000);
    }

    public String build(long timeInSecs) {
        return listName + "." + timeInSecs;
    }
}
