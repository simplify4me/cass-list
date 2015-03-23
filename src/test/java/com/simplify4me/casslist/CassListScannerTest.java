package com.simplify4me.casslist;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.simplify4me.casslist.support.CassListCF;
import com.simplify4me.casslist.support.CassListEntryHandler;
import com.simplify4me.casslist.support.CassListScanner;
import com.simplify4me.casslist.support.TimeBasedRWPolicy;
import com.simplify4me.casslist.support.TimeInSec;

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
public class CassListScannerTest {

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
    public void testScanner() throws Exception {
        CassListCF cassListCF = new CassListCF(context.getClient(), "llist");

        final int numValues = 5;
        final int numReaders = 3;

        final AtomicLong numDone = new AtomicLong(0);

        final Map<String, String> trackingMap = new ConcurrentHashMap<>();
        final Map<String, AtomicLong> totalRead = new ConcurrentHashMap<>();

        final Set<String> valueSet = writeABunchOfValues(numValues);
        System.out.println("wrote=" + valueSet);

        final CassListEntryHandler handler = listEntries -> {
            if (listEntries != null) {
                System.out.println(listEntries);
                if (trackingMap.containsKey(listEntries.getReferenceID())) {
                    System.err.println("existing key listEntries=" + listEntries.getReferenceID());
                }
                trackingMap.put(listEntries.getReferenceID(), listEntries.getReaderName());
                final AtomicLong count = totalRead.computeIfAbsent(listEntries.getReaderName(), f -> new AtomicLong(0));
                for (Iterator<String> itr = listEntries.iterator(); itr.hasNext(); ) {
                    final String next = itr.next();
//                            System.out.printf("cons %s, next=%s", readerName, next);
                    if (valueSet.contains(next)) count.incrementAndGet();
                }

                if (count.get() >= numValues) {
                    numDone.incrementAndGet();
                }
            }
        };

        CassListScanner scanner = new CassListScanner.Builder(cassListCF, "default")
            .withCassListEntryHandler(handler)
            .withEntryExpiryInSecs(60)
            .withLookback(5)
            .withReaderNamePrefix("Reader")
            .withNumReaders(numReaders)
            .build();

        do {
            Thread.sleep(1000);
            System.out.println(totalRead);
        }
        while (numDone.get() < numReaders);

        scanner.destroy();
        System.out.println("total read=" + totalRead);
    }

    Set<String> writeABunchOfValues(int numValues) throws Exception {
        CassListCF cassListCF = new CassListCF(context.getClient(), "llist");

        final CassListRWPolicy readPolicy = new TimeBasedRWPolicy(TimeInSec.minusSecs(5));
        CassList cassList = new SimpleCassList("default", cassListCF, readPolicy);

        return CassListTestHelper.writeABunchOfValues(cassList, numValues);
    }
}
