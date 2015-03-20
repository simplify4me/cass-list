package com.simplify4me.casslist.support;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 */
public class TimeInSec extends TimerTask {

    private volatile long now = System.currentTimeMillis()/1000;

    private TimeInSec() {
        final Timer timer = new Timer("TimerInSec", true);
        timer.schedule(this, 1, 1);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                timer.cancel();
            }
        });
    }

    public static long now() {
        return Holder.INSTANCE.now;
    }

    public static long minusSecs(long value) {
        return minus(value, TimeUnit.SECONDS);
    }

    public static long minus(long value, TimeUnit timeUnit) {
        return now() - TimeUnit.SECONDS.convert(value, timeUnit);
    }

    @Override
    public void run() {
        now = System.currentTimeMillis()/1000;
    }

    static class Holder {
        static final TimeInSec INSTANCE = new TimeInSec();
    }
}
