package com.simplify4me.casslist.support;

/**
 *
 */
public final class TimeBasedCassListIndexBuilder {
    private final String listName;

    public TimeBasedCassListIndexBuilder() {
        this("default");
    }

    public TimeBasedCassListIndexBuilder(String listName) {
        this.listName = listName;
    }

    public String build(long timeInSecs) {
        return listName + "." + timeInSecs;
    }
}
