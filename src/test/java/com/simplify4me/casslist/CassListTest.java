package com.simplify4me.casslist;

import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.simplify4me.casslist.support.CassListCF;
import com.simplify4me.casslist.support.TimeBasedCassListIndexBuilder;
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

        CassList cassList = new TimeBasedCassList(cassListCF, new TimeBasedCassListIndexBuilder());
        writeABunchOfValues(cassList, numValues);

        int totalRead = 0;

        for (CassListEntries entry = cassList.read(); entry != null; entry = cassList.read()) {
            System.out.println("eid=" + entry.getReferenceID());
            cassList.markAsRead(entry);
            totalRead += entry.size();
            System.out.println(entry);
        }

        Assert.assertTrue(totalRead >= numValues);
    }

//    @Test
//    public void testConcurrentRW() throws Exception {
//        final Keyspace ks = context.getClient();
//
//        final int numValues = 10;
//
//        TimeBasedCassList cassList = new TimeBasedCassList(ks, "llist");
//        new ConcurrentCassListReader(cassList).read(new CassListEntryHandler() {
//            @Override
//            public void handle(CassListEntry entry) {
//                System.out.println("read=" + entry);
//            }
//        });
//
//        writeABunchOfValues(cassList, numValues);
//    }

    private static void writeABunchOfValues(CassList cassList, int numValues) throws Exception {
        Random random = new Random();
        for (int index = 0; index < 10; index++) {
            cassList.add(String.valueOf(index));
            Thread.sleep(random.nextInt(1000));
        }
        System.out.println("done writing");
    }
}
