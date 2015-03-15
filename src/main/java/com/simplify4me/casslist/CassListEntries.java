package com.simplify4me.casslist;

import java.util.Collections;
import java.util.Map;

/**
 * A collection of entries in the CassList
 */
public class CassListEntries {
    private String referenceID = null;
    private Map<String, String> entries = null;

    public CassListEntries(String referenceID, Map<String, String> entries) {
        this.referenceID = referenceID;
        this.entries = entries;
    }

    public String getReferenceID() {
        return referenceID;
    }

    public Map<String, String> getEntries() {
        return entries == null ? Collections.emptyMap() : entries;
    }

    public int size() {
        return getEntries().size();
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
