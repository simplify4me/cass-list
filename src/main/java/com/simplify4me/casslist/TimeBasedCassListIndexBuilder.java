package com.simplify4me.casslist;

import java.util.Date;

import org.apache.log4j.Logger;

/**
 *
 */
public final class TimeBasedCassListIndexBuilder implements CassListIndexBuilder {
    private static final Logger logger = Logger.getLogger(TimeBasedCassListIndexBuilder.class);

    private final String listName;

    public TimeBasedCassListIndexBuilder() {
        this("default");
    }

    public TimeBasedCassListIndexBuilder(String listName) {
        this.listName = listName;
    }

    @Override
    public String build() {
        final long now = System.currentTimeMillis() / 1000;
        if (logger.isDebugEnabled()) {
            logger.debug("build=" + new Date(now*1000));
        }
        return build(now);
    }

    public String build(long timeInSecs) {
        return listName + "." + timeInSecs;
    }
}
