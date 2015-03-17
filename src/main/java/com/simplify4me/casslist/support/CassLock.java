package com.simplify4me.casslist.support;

import java.util.UUID;

import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.util.TimeUUIDUtils;

/**
 * Helper class
 */
public class CassLock {

    private final UUID lockID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
    private final String lockValue = lockID.toString();

    private Integer lockExpiryInSecs = null;
    private BaseCF<String, UUID> cassCF = null;

    public CassLock(BaseCF<String, UUID> cassCF, Integer lockExpiryInSecs) {
        this.cassCF = cassCF;
        this.lockExpiryInSecs = lockExpiryInSecs;
    }

    public boolean tryLock(String rowToLock) throws ConnectionException {
        final String lockRowKey = rowToLock + ".lock";
        if (lockValue.equals(readLock(lockRowKey))) { //do i have the lock?
            writeLock(lockRowKey); //refresh the lock i.e. extend ttl
            return true;
        }
        else { //i don't have the lock, try-lock
            writeLock(lockRowKey);
            if (lockValue.equals(readLock(lockRowKey))) return true;
            else {
                dropLock(lockRowKey);
                return false;
            }
        }
    }

    private void dropLock(String lockRowKey) throws ConnectionException {
        final MutationBatch mutationBatch = cassCF.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
        mutationBatch.withRow(cassCF, lockRowKey).deleteColumn(lockID);
        mutationBatch.executeAsync();
    }

    private void writeLock(String lockRowKey) throws ConnectionException {
        MutationBatch mutationBatch = cassCF.prepareMutationBatch(ConsistencyLevel.CL_LOCAL_QUORUM);
        mutationBatch.withRow(cassCF, lockRowKey).putColumn(lockID, lockValue, lockExpiryInSecs);
        mutationBatch.execute();
    }

    private String readLock(String lockRowKey) throws ConnectionException {
        final OperationResult<ColumnList<UUID>> result = cassCF.prepareQuery()
            .getKey(lockRowKey).execute();
        if (result != null && result.getResult().size() == 1) {
            final Column<UUID> columnByName = result.getResult().getColumnByName(lockID);
            if (columnByName != null) {
                return columnByName.getStringValue();
            }
        }
        return null;
    }
}
