package com.simplify4me.casslist;

import java.util.Map;
import java.util.Random;
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

        final TimeBasedCassListIndexBuilder indexBuilder = new TimeBasedCassListIndexBuilder("default");

        CassListReadPolicy readPolicy = new LookbackInTimeReadPolicy(indexBuilder, (System.currentTimeMillis()/1000)-1);
        LockedListReadPolicy lockedListReadPolicy = new LockedListReadPolicy(cassListCF, readPolicy, "default");
        lockedListReadPolicy.setNumLocks(3);

        CassList cassList = new SimpleCassList(cassListCF, lockedListReadPolicy, indexBuilder);
        writeABunchOfValues(cassList, numValues);

        final AtomicLong totalRead = new AtomicLong(0);

        final ExecutorService executorService = Executors.newFixedThreadPool(5);
        final Map<String, String> trackingMap = new ConcurrentHashMap<>();

        final Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    final String consumer = Thread.currentThread().getId() + ".C";
                    final CassListEntries read = cassList.read(consumer);
                    if (read != null) {
                        System.out.println(read);
                        if (trackingMap.containsKey(read.getReferenceID())) {
                            System.err.println("existing key read=" + read.getReferenceID());
                        }
                        trackingMap.put(read.getReferenceID(), consumer);
                        totalRead.addAndGet(read.size());
                        cassList.markAsRead(read);
                    }

                }
                catch (ConnectionException e) {
                    e.printStackTrace();
                }
            }
        };

        while (totalRead.get() < numValues) {
            for (int index = 0; index < 3; index++) executorService.submit(task);
            Thread.sleep(1000);
        }

        executorService.shutdown();
        executorService.awaitTermination(15, TimeUnit.SECONDS);
        System.out.println("total read=" + totalRead.get());
    }

    private static void writeABunchOfValues(CassList cassList, int numValues) throws Exception {
        Random random = new Random();
        for (int index = 0; index < 10; index++) {
            cassList.add(String.valueOf(index));
            Thread.sleep(random.nextInt(1000));
        }
        System.out.println("done writing");
    }
}
