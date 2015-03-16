package com.simplify4me.casslist;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.netflix.astyanax.model.Column;

/**
 * A collection of entries in the CassList
 */
public class CassListEntries {
    private final String referenceID;
    private final String consumerName;
    private final Set<Column<UUID>> entries;

    public CassListEntries(@Nonnull String consumerName, @Nonnull String referenceID, @Nonnull Set<Column<UUID>> entries) {
        this.consumerName = consumerName;
        this.referenceID = referenceID;
        this.entries = entries;
    }

    public String getReferenceID() {
        return referenceID;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public Iterator<String> iterator() {
        final Iterator<Column<UUID>> delegate = entries.iterator();
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public String next() {
                final Column<UUID> next = delegate.next();
                return next.getStringValue();
            }
        };
    }

    Set<Column<UUID>> getEntries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassListEntry{");
        sb.append("referenceID='").append(referenceID).append('\'');
        sb.append(", entries=").append(String.valueOf(entries));
        sb.append('}');
        return sb.toString();
    }
}
