package com.simplify4me.casslist.support;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.simplify4me.casslist.CassList;
import com.simplify4me.casslist.CassListEntries;
import com.simplify4me.casslist.SimpleCassList;
import com.sun.istack.internal.NotNull;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 */
public final class CassListScanner {

    private final String readerNamePrefix;
    private ExecutorService executors = null;

    private CassListScanner(@NotNull String readerNamePrefix) {
        this.readerNamePrefix = readerNamePrefix;
    }

    void init(Builder builder, CassList defaultList, CassList... otherLists) {
        executors = Executors.newFixedThreadPool(builder.numReaders + otherLists.length);

        for (int index = 0; index < builder.numReaders; index++) {
            executors.submit(new ScannerAndNotifier(readerNamePrefix + "-" + index, defaultList, builder.handler));
        }

        int readerSuffix = builder.numReaders;
        for (CassList otherList : otherLists) {
            executors.submit(new ScannerAndNotifier(readerNamePrefix + "-" + readerSuffix, otherList, builder.handler));
            readerSuffix++;
        }
    }

    public void destroy() throws InterruptedException {
        if (this.executors != null) {
            this.executors.shutdown();
            this.executors.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    class ScannerAndNotifier implements Runnable {

        private final String readerName;
        private final CassList cassList;
        private final CassListEntryHandler entryHandler;

        ScannerAndNotifier(String readerName, CassList cassList, CassListEntryHandler entryHandler) {
            this.readerName = readerName;
            this.cassList = cassList;
            this.entryHandler = entryHandler;
        }

        @Override
        public void run() {
            while (!executors.isShutdown()) {
                try {
                    final CassListEntries entry = cassList.read(readerName);
                    if (entry != null) {
                        notifyHandler(entry);
                    }
                }
                catch (ConnectionException e) {
                    e.printStackTrace();
                }
            }
        }

        private void notifyHandler(CassListEntries entry) throws ConnectionException {
            try {
                entryHandler.handle(entry);
                cassList.markAsRead(entry);
            }
            catch (ConnectionException ex) {
                throw ex;
            }
            catch (Exception e) {
                e.printStackTrace(); //ignore and continue;
            }
        }
    }

    public static class Builder {

        private int numReaders = 1;

        private long lookbackSecs = 5; //5 second default
        private long entryExpiryInSecs = 3600; //1 hour default

        private String readerNamePrefix = null;
        private CassListEntryHandler handler = null;

        private final String listName;
        private final CassListCF cassListCF;

        public Builder(@NotNull CassListCF cassListCF, @NotNull String listName) {
            this.cassListCF = cassListCF;
            this.listName = listName;
        }

        public Builder withLookback(long lookbackSecs) {
            this.lookbackSecs = lookbackSecs;
            return this;
        }

        public Builder withReaderNamePrefix(@NotNull String readerNamePrefix) {
            this.readerNamePrefix = readerNamePrefix;
            return this;
        }

        public Builder withNumReaders(int numReaders) {
            this.numReaders = numReaders;
            return this;
        }

        public Builder withEntryExpiryInSecs(long expiryInSecs) {
            this.entryExpiryInSecs = expiryInSecs;
            return this;
        }

        public Builder withCassListEntryHandler(CassListEntryHandler handler) {
            this.handler = handler;
            return this;
        }

        public CassListScanner build() {
            CassLock cassLock = new CassLock(cassListCF, Integer.valueOf(30)); //5 mins

            TimeBasedRWPolicy rwPolicy1 = new TimeBasedRWPolicy(TimeInSec.minusSecs(lookbackSecs));
            CassList defaultList = new SimpleCassList(listName, cassListCF, new LockedListRWPolicy(cassLock, rwPolicy1));

            TimeBasedRWPolicy rwPolicy2 = new TimeBasedRWPolicy(TimeInSec.minusSecs(lookbackSecs*3), rwPolicy1);
            CassList recentsList = new SimpleCassList(listName, cassListCF, new LockedListRWPolicy(cassLock, rwPolicy2));

            TimeBasedRWPolicy rwPolicy3 = new TimeBasedRWPolicy(TimeInSec.minusSecs(entryExpiryInSecs), rwPolicy2);
            CassList fullList = new SimpleCassList(listName, cassListCF, new LockedListRWPolicy(cassLock, rwPolicy3));

            //START....Build and init CassListScanner
            CassListScanner scanner = new CassListScanner(readerNamePrefix);
            scanner.init(this, defaultList, recentsList, fullList);

            return scanner;
        }
    }
}
