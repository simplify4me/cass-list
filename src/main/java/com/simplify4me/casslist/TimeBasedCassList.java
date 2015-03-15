package com.simplify4me.casslist;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.simplify4me.casslist.support.CassListCF;
import com.simplify4me.casslist.support.TimeBasedCassListIndexBuilder;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Time indexed list (i.e. list indicies are timestamps) where values are stored at a
 * 'Second' granularity. Multiple values can be stored at every index
 */
public class TimeBasedCassList implements CassList {

    private static final Integer FIVE_MINS_IN_SECS = Integer.valueOf(600);
    private static final Integer ONE_SEC = Integer.valueOf(1);

    private Integer entryExpiryInSecs = FIVE_MINS_IN_SECS;

    private final CassListCF cassList;
    private final TimeBasedCassListIndexBuilder indexBuilder;

    private CassListReadPolicy defaultReadPolicy = null;

    public TimeBasedCassList(CassListCF cassListCF, TimeBasedCassListIndexBuilder indexBuilder) throws ConnectionException {
        this.cassList = cassListCF;
        this.indexBuilder = indexBuilder;
        this.defaultReadPolicy = TimeBasedCassListReadPolicy.LookbackInTimeReadPolicy.lookback5MinsPolicy(indexBuilder);
    }

    @Override
    public void setEntryExpiryInSecs(int entryExpiryInSecs) {
        this.entryExpiryInSecs = Integer.valueOf(entryExpiryInSecs);
    }

    @Override
    public void setDefaultReadPolicy(TimeBasedCassListReadPolicy readPolicy) {
        this.defaultReadPolicy = readPolicy;
    }

    @Override
    public String add(String value) throws ConnectionException {
        final MutationBatch mutationBatch = cassList.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
        final String rowKey = add(mutationBatch, value);
        mutationBatch.execute();
        return rowKey;
    }

    @Override
    public String add(MutationBatch mutationBatch, String value) throws ConnectionException {
        final String rowKey = indexBuilder.build();
        final UUID timeUUID = TimeUUIDUtils.getTimeUUID(System.currentTimeMillis());
        mutationBatch.withRow(cassList, rowKey)
            .putColumn(timeUUID, value, entryExpiryInSecs);
        return rowKey;
    }

    @Override
    public CassListEntries read() throws ConnectionException {
        return read(defaultReadPolicy);
    }

    @Override
    public CassListEntries read(CassListReadPolicy readPolicy) throws ConnectionException {
        String rowKey = readPolicy.nextRowToRead();
        while (rowKey != null) {
            //System.out.println("rk=" + rowKey);
            final Set<Column<UUID>> unreadRow = getUnreadRow(rowKey);
            if (!unreadRow.isEmpty()) {
                return new CassListEntries(rowKey, unreadRow);
            }
            rowKey = readPolicy.nextRowToRead();
        }
        return null;
    }

    protected Set<Column<UUID>> getUnreadRow(String rowKey) throws ConnectionException {
        Set<Column<UUID>> entries = null;
        final RowQuery<String, UUID> key = cassList.prepareQuery(ConsistencyLevel.CL_LOCAL_QUORUM).getKey(rowKey);
        final ColumnList<UUID> result = key.execute().getResult();
        if (result.size() > 0) {
            for (int index = 0; index < result.size(); index++) {
                final Column<UUID> column = result.getColumnByIndex(index);
                if (column.getStringValue() != null && !"null".equals(column.getStringValue())) {
                    if (entries == null) entries = new HashSet<>(result.size(), 1.0f);
                    System.out.println("ttl=" + column.getTtl());
                    entries.add(column);
                }
            }
        }
        return entries == null ? Collections.emptySet() : entries;
    }

    @Override
    public void markAsRead(CassListEntries entry) throws ConnectionException {
        if (entry != null) {
            final MutationBatch mutationBatch = cassList.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
            final ColumnListMutation<UUID> mutation = mutationBatch.withRow(cassList, entry.getReferenceID());
            entry.getEntries().forEach(e -> mutation.putColumn(e.getName(), "null", ONE_SEC));
            mutationBatch.execute();
        }
    }
}
