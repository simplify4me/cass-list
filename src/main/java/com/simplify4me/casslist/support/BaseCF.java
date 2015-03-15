package com.simplify4me.casslist.support;

import java.util.Date;

import org.apache.cassandra.thrift.Mutation;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;

/**
 * Helper class
 */
public class BaseCF<K,C> extends ColumnFamily<K, C> {

    private Keyspace keyspace = null;

    public BaseCF(Keyspace keyspace, String columnFamilyName, Serializer<K> keySerializer, Serializer<C> columnSerializer) {
        super(columnFamilyName, keySerializer, columnSerializer);
        this.keyspace = keyspace;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public ColumnFamilyQuery<K, C> prepareQuery(ConsistencyLevel consistencyLevel) {
        ColumnFamilyQuery<K, C> cfQuery = getKeyspace().prepareQuery(this);
        cfQuery.setConsistencyLevel(consistencyLevel);
        return cfQuery;
    }

    public MutationBatch prepareMutationBatch(ConsistencyLevel consistencyLevel) {
        MutationBatch batch = getKeyspace().prepareMutationBatch();
        batch.setConsistencyLevel(consistencyLevel);
        return batch;
    }

    public void setIfNotNull(ColumnListMutation<String> cm, String columnName, Object o) {
        if (o != null) {
            if (o instanceof Date) {
                cm.putColumn(columnName, ((Date) o).getTime(), null);
            }
            else if (o instanceof Long) {
                cm.putColumn(columnName, ((Long) o).longValue(), null);
            }
            else if (o instanceof Integer) {
                cm.putColumn(columnName, ((Integer) o).intValue(), null);
            }
            else if (o instanceof Float) {
                cm.putColumn(columnName, ((Float) o).floatValue(), null);
            }
            else if (o instanceof Boolean) {
                cm.putColumn(columnName, ((Boolean) o).booleanValue(), null);
            }
            else {
                cm.putColumn(columnName, o.toString(), null);
            }
        }
    }

    public ColumnList<C> getRow(K key, ConsistencyLevel level)
        throws ConnectionException
    {
        OperationResult<ColumnList<C>> result = prepareQuery(level).getKey(key).execute();
        if (result.getResult() == null || result.getResult().isEmpty()) {
            return null;
        }
        else return result.getResult();
    }

    public String getString(ColumnList<String> columns, String columnName) {
        Column<String> column = columns.getColumnByName(columnName);
        if (column != null) {
            return column.getStringValue();
        }
        return null;
    }

    public long getLong(ColumnList<String> columns, String columnName) {
        Column<String> column = columns.getColumnByName(columnName);
        if (column != null) {
            return column.getLongValue();
        }
        return 0;
    }

    public int getInt(ColumnList<String> columns, String columnName) {
        Column<String> column = columns.getColumnByName(columnName);
        if (column != null) {
            return (int) column.getIntegerValue();
        }
        return 0;
    }

    public boolean getBoolean(ColumnList<String> columns, String columnName) {
        Column<String> column = columns.getColumnByName(columnName);
        if (column != null) {
            return column.getBooleanValue();
        }
        return false;
    }

    public Date getDate(ColumnList<String> columns, String columnName) {
        Column<String> column = columns.getColumnByName(columnName);
        if (column != null) {
            return new Date(column.getLongValue());
        }
        return null;
    }
}
