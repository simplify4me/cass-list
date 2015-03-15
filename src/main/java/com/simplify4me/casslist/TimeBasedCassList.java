package com.simplify4me.casslist;

import java.util.HashMap;
import java.util.Map;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Time indexed list (i.e. list indicies are timestamps) where values are stored at a
 * 'Second' granularity. Multiple values can be stored at every index
 */
public class TimeBasedCassList implements CassList {

    private static final Integer FIVE_MINS_IN_SECS = Integer.valueOf(600);
    private static final String ROW_READ_COL = "cl.read";

    private Integer entryExpiryInSecs = FIVE_MINS_IN_SECS;

    private final String listName;
    private final Keyspace keyspace;
    private final ColumnFamily<String, String> cassList;

    private TimeBasedCassListReadPolicy defaultReadPolicy = null;

    public TimeBasedCassList(Keyspace keyspace, String listCFName) throws ConnectionException {
        this(keyspace, listCFName, "default");
    }

    public TimeBasedCassList(Keyspace keyspace, String listCFName, String listName) throws ConnectionException {
        this.keyspace = keyspace;
        this.listName = listName;
        this.cassList = new ColumnFamily<>(listCFName, StringSerializer.get(), StringSerializer.get());
        this.defaultReadPolicy = new TimeBasedCassListReadPolicy.LookbackInTimeReadPolicy(currentRowKey() - FIVE_MINS_IN_SECS.intValue());
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
    public String add(String key, String value) throws ConnectionException {
        final MutationBatch mutationBatch = keyspace.prepareMutationBatch();
        final String rowKey = add(mutationBatch, key, value);
        mutationBatch.execute();
        return rowKey;
    }

    @Override
    public String add(MutationBatch mutationBatch, String key, String value) throws ConnectionException {
        final String rowKey = listName + currentRowKey();
        mutationBatch.withRow(cassList, rowKey)
            .putColumn(key, value, entryExpiryInSecs)
            .putColumn(ROW_READ_COL, false, entryExpiryInSecs);
        return rowKey;
    }

    protected long currentRowKey() {
        return System.currentTimeMillis()/1000;
    }

    @Override
    public CassListEntries read() throws ConnectionException {
        return read(defaultReadPolicy);
    }

    @Override
    public CassListEntries read(TimeBasedCassListReadPolicy readPolicy) throws ConnectionException {
        long rowToRead = readPolicy.nextRowToRead();
        while (rowToRead > 0) {
            final String rowKey = listName + rowToRead;
            final ColumnList<String> unreadRow = getUnreadRow(rowKey);
            if (unreadRow != null && !unreadRow.isEmpty()) {
                return new CassListEntries(rowKey, buildMap(unreadRow));
            }
            rowToRead = readPolicy.nextRowToRead();
        }
        return null;
    }

    private Map<String, String> buildMap(final ColumnList<String> unreadRow) {
        Map<String, String> entries = new HashMap<>(unreadRow.size(), 1.0f);
        for (int col = 0; col < unreadRow.size(); col++) {
            final Column<String> column = unreadRow.getColumnByIndex(col);
            if (!ROW_READ_COL.equals(column.getName())) {
                entries.put(column.getName(), column.getStringValue());
            }
        }
        return entries;
    }

    protected ColumnList<String> getUnreadRow(String rowKey) throws ConnectionException {
        final RowQuery<String, String> key = keyspace.prepareQuery(cassList).getKey(rowKey);
        final ColumnList<String> result = key.execute().getResult();
        if (result.size() > 0) {
            final Column<String> columnByName = result.getColumnByName(ROW_READ_COL);
            if (columnByName != null && !columnByName.getBooleanValue()) {
                return result;
            }
        }
        return null;
    }

    @Override
    public void markAsRead(CassListEntries entry) throws ConnectionException {
        if (entry != null) ack(entry.getReferenceID());
    }

    protected void ack(String rowKey) throws ConnectionException {
        final MutationBatch mutationBatch = keyspace.prepareMutationBatch();
        mutationBatch.withRow(cassList, rowKey).putColumn(ROW_READ_COL, true);
        mutationBatch.execute();
    }
}
