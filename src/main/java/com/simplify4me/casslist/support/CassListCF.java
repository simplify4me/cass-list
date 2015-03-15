package com.simplify4me.casslist.support;


import java.util.UUID;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;

/**
 * Helper class
 */
public final class CassListCF extends BaseCF<String, UUID> {
    public CassListCF(Keyspace keyspace, String columnFamilyName) {
        super(keyspace, columnFamilyName, StringSerializer.get(), TimeUUIDSerializer.get());
    }
}
