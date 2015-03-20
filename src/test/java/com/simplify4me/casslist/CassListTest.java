package com.simplify4me.casslist;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.simplify4me.casslist.support.CassListCF;
import com.simplify4me.casslist.support.LookbackInTimeReadPolicy;
import com.simplify4me.casslist.support.TimeInSec;
import junit.framework.Assert;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Unit test for simple App.
 */
public class CassListTest {

    static AstyanaxContext<Keyspace> context = null;

    @BeforeClass
    public static void init() {
        context = new AstyanaxContext.Builder()
            .forCluster("localhost")
            .forKeyspace("test")
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                           .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                                 .setPort(9160)
                                                 .setMaxConnsPerHost(1)
                                                 .setSeeds("127.0.0.1:9160")
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
    }

    @AfterClass
    public static void shutdown() {
        if (context != null) context.shutdown();
    }

    @Test
    public void testReadWrite() throws Exception {
        CassListCF cassListCF = new CassListCF(context.getClient(), "llist");

        final int numValues = 10;

        final TimeBasedCassListIndexBuilder indexBuilder = new TimeBasedCassListIndexBuilder("default");

        final long startFromSecs = TimeInSec.minusSecs(5);
        final CassListReadPolicy readPolicy = new LookbackInTimeReadPolicy(indexBuilder, startFromSecs);

        CassList cassList = new SimpleCassList(cassListCF, readPolicy, indexBuilder);
        CassListTestHelper.writeABunchOfValues(cassList, numValues);

        int totalRead = 0;

        for (CassListEntries entry = cassList.read("t1"); totalRead < numValues; entry = cassList.read("t1")) {
            if (entry == null) continue;
            System.out.println("eid=" + entry.getReferenceID());
            cassList.markAsRead(entry);
            totalRead += entry.size();
            System.out.println(entry);
        }

        Assert.assertTrue(totalRead >= numValues);
    }

}
