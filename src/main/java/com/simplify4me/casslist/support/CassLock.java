package com.simplify4me.casslist.support;

import java.util.UUID;

import org.apache.log4j.Logger;

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

    private static final Logger logger = Logger.getLogger(CassLock.class);

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
        final LockStatus lockStatus = getLockStatus(lockRowKey);
        if (logger.isDebugEnabled()) {
            logger.debug("tryLock=" + lockRowKey + "; lockStatus=" + String.valueOf(lockStatus));
        }
        switch (lockStatus) {
            case OWN_LOCK:
                writeLock(lockRowKey); //refresh the lock i.e. extend ttl
                return true;
            case NONE:
                //i don't have the lock, try-lock
                writeLock(lockRowKey);
                if (getLockStatus(lockRowKey) == LockStatus.OWN_LOCK) {
                    return true;
                }
                else {
                    dropLock(lockRowKey);
                    return false;
                }
            case MULTI_LOCK:
            default:
                return false;
        }
    }

    private void dropLock(String lockRowKey) throws ConnectionException {
        final MutationBatch mutationBatch = cassCF.prepareMutationBatch();
        mutationBatch.withRow(cassCF, lockRowKey).deleteColumn(lockID);
        mutationBatch.execute();
    }

    private void writeLock(String lockRowKey) throws ConnectionException {
        MutationBatch mutationBatch = cassCF.prepareMutationBatch();
        mutationBatch.withRow(cassCF, lockRowKey).putColumn(lockID, lockValue, lockExpiryInSecs);
        mutationBatch.execute();
    }

    private LockStatus getLockStatus(String lockRowKey) throws ConnectionException {
        final OperationResult<ColumnList<UUID>> result = cassCF.prepareQuery()
            .getKey(lockRowKey).execute();
        if (result != null) {
            if (result.getResult().size() == 1) {
                final Column<UUID> columnByName = result.getResult().getColumnByName(lockID);
                if (columnByName != null) {
                    if (lockValue.equals(columnByName.getStringValue())) {
                        return LockStatus.OWN_LOCK;
                    }
                }
            }
            else if (result.getResult().size() > 1) {
                return LockStatus.MULTI_LOCK;
            }
        }
        return LockStatus.NONE;
    }

    enum LockStatus {
        OWN_LOCK,
        MULTI_LOCK,
        NONE
    }
}
