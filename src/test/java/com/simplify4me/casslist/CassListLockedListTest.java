package com.simplify4me.casslist;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.simplify4me.casslist.support.CassListCF;
import com.simplify4me.casslist.support.LockedListReadPolicy;
import com.simplify4me.casslist.support.LookbackInTimeReadPolicy;
import com.simplify4me.casslist.support.TimeBasedCassListBuilder;
import com.simplify4me.casslist.support.TimeInSec;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Unit test for simple App.
 */
public class CassListLockedListTest {

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
        final int numReaders = 4;

        CassList cassList = new TimeBasedCassListBuilder(cassListCF, "default")
            .withLookback(5).build();

        final Set<String> valueSet = CassListTestHelper.writeABunchOfValues(cassList, numValues);
        System.out.println("wrote=" + valueSet);
        final ExecutorService executorService = Executors.newFixedThreadPool(numReaders);

        final Map<String, String> trackingMap = new ConcurrentHashMap<>();
        final Map<String, AtomicLong> totalRead = new ConcurrentHashMap<>();

        final Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    final String readerName = "Reader-" + Thread.currentThread().getId();
                    final CassListEntries read = cassList.read(readerName);
                    if (read != null) {
                        System.out.println(read);
                        if (trackingMap.containsKey(read.getReferenceID())) {
                            System.err.println("existing key read=" + read.getReferenceID());
                        }
                        trackingMap.put(read.getReferenceID(), readerName);
                        final AtomicLong count = totalRead.computeIfAbsent(readerName, f -> new AtomicLong(0));
                        for (Iterator<String> itr = read.iterator(); itr.hasNext(); ) {
                            final String next = itr.next();
//                            System.out.printf("cons %s, next=%s", readerName, next);
                            if (valueSet.contains(next)) count.incrementAndGet();
                        }
                        cassList.markAsRead(read);
                    }
                }
                catch (ConnectionException e) {
                    e.printStackTrace();
                }
            }
        };

        int numDone = 0;
        do {
            for (int index = 0; index < numReaders; index++) executorService.submit(task);
            Thread.sleep(1000);
            for (AtomicLong count : totalRead.values()) {
                if (count.get() >= numValues) {
                    numDone++;
                    break;
                }
            }
            System.out.println(totalRead);
        }
        while (numDone < numReaders);

        executorService.shutdown();
        executorService.awaitTermination(15, TimeUnit.SECONDS);
        System.out.println("total read=" + totalRead);
    }
}
