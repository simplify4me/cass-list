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

    public String build() {
        return build(System.currentTimeMillis());
    }

    public String build(long timeInMillis) {
        return listName + "." + (timeInMillis/1000);
    }
}
