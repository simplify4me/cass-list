package com.simplify4me.casslist.support;

import com.simplify4me.casslist.CassList;
import com.simplify4me.casslist.SimpleCassList;
import com.simplify4me.casslist.TimeBasedCassListIndexBuilder;

/**
*/
public class TimeBasedCassListBuilder {

    private long lookbackSecs = 5;

    private final String listName;
    private final CassListCF cassListCF;

    public TimeBasedCassListBuilder(CassListCF cassListCF, String listName) {
        this.cassListCF = cassListCF;
        this.listName = listName;
    }

    public TimeBasedCassListBuilder withLookback(long lookbackSecs) {
        this.lookbackSecs = lookbackSecs;
        return this;
    }

    public CassList build() {
        final TimeBasedCassListIndexBuilder indexBuilder = new TimeBasedCassListIndexBuilder(listName);

        final LookbackInTimeReadPolicy readPolicy = new LookbackInTimeReadPolicy(indexBuilder,
                                                                                 TimeInSec.minusSecs(lookbackSecs));

        LockedListReadPolicy lockedListReadPolicy = new LockedListReadPolicy(cassListCF, readPolicy, listName);

        return new SimpleCassList(cassListCF,
                                  lockedListReadPolicy,
                                  indexBuilder);
    }
}
