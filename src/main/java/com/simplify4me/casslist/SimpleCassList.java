package com.simplify4me.casslist;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.simplify4me.casslist.support.CassListCF;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * A configurable CassList implementation that supports the optional operation of tracking
 * entries that have been read.
 *
 */
public class SimpleCassList implements CassList {

    protected Integer entryExpiryInSecs = Integer.valueOf(600);

    protected final CassListCF cassListCF;
    protected final CassListReadPolicy readPolicy;
    protected final CassListIndexBuilder indexBuilder;

    public SimpleCassList(CassListCF cassListCF, CassListReadPolicy readPolicy, CassListIndexBuilder indexBuilder) {
        this.cassListCF = cassListCF;
        this.readPolicy = readPolicy;
        this.indexBuilder = indexBuilder;
    }

    @Override
    public void setEntryExpiryInSecs(int entryExpiryInSecs) {
        this.entryExpiryInSecs = Integer.valueOf(entryExpiryInSecs);
    }

    @Override
    public String add(@Nonnull String value) throws ConnectionException {
        final MutationBatch mutationBatch = cassListCF.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
        final String rowKey = add(mutationBatch, value);
        mutationBatch.execute();
        return rowKey;
    }

    @Override
    public String add(@Nonnull MutationBatch mutationBatch, @Nonnull String value) throws ConnectionException {
        final String rowKey = indexBuilder.build();
        final UUID timeUUID = TimeUUIDUtils.getUniqueTimeUUIDinMicros();
        mutationBatch.withRow(cassListCF, rowKey)
            .putColumn(timeUUID, value, entryExpiryInSecs);
        return rowKey;
    }

    @Override
    public CassListEntries read(@Nonnull String readerName) throws ConnectionException {
        final String rowToRead = this.readPolicy.nextRowToRead(readerName);
        if (rowToRead != null) {
            String trackingKey = buildTrackingRowKey(readerName, rowToRead);
            final OperationResult<Rows<String, UUID>> result = this.cassListCF.prepareQuery()
                .getKeySlice(trackingKey, rowToRead).execute();
            if (result != null && !result.getResult().isEmpty()) {
                final Row<String, UUID> trackingRow = result.getResult().getRow(trackingKey);
                final Row<String, UUID> dataRow = result.getResult().getRow(rowToRead);

                Set<UUID> dataCols = new HashSet<>(dataRow.getColumns().getColumnNames());
                dataCols.removeAll(trackingRow.getColumns().getColumnNames());

                if (!dataCols.isEmpty()) {
                    Set<Column<UUID>> set = new HashSet<>(dataCols.size(), 1.0f);
                    dataCols.forEach(c -> set.add(dataRow.getColumns().getColumnByName(c)));
                    return new CassListEntries(readerName, rowToRead, set);
                }
            }
        }
        return null;
    }

    private String buildTrackingRowKey(String readerName, String rowToRead) {
        return rowToRead + ".reader." + readerName;
    }

    @Override
    public void markAsRead(@Nonnull CassListEntries entry) throws ConnectionException {
        if (entry.size() == 0) return;
        final String trackingKey = buildTrackingRowKey(entry.getReaderName(), entry.getReferenceID());
        final MutationBatch mutationBatch = cassListCF.prepareMutationBatch();
        final ColumnListMutation<UUID> mutation = mutationBatch.withRow(cassListCF, trackingKey);
        entry.getEntries().forEach(e -> mutation.putColumn(e.getName(), 1, entryExpiryInSecs));
        mutationBatch.execute();
    }
}
