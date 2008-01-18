/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;

import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.test.TestBase;

/**
 * Tests the database file locking facility.
 * Both lock files and sockets locking is tested.
 */
public class TestFileLock extends TestBase implements Runnable {

    int wait;
    static final int KILL = 5;
    static final String FILE = baseDir + "/test.lock";

    private boolean allowSockets;
    private static volatile int locks;
    private static volatile boolean stop;

    public TestFileLock() {
    }

    public void test() throws Exception {
        test(false);
        test(true);
    }

    void test(boolean allowSockets) throws Exception {
        int threadCount = getSize(3, 5);
        wait = getSize(20, 200);
        Thread[] threads = new Thread[threadCount];
        new File(FILE).delete();
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(new TestFileLock(this, allowSockets));
            threads[i].start();
            Thread.sleep(wait + (int) (Math.random() * wait));
        }
        trace("wait");
        Thread.sleep(500);
        stop = true;
        trace("STOP file");
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        check(locks, 0);
    }

    TestBase base;

    TestFileLock(TestBase base, boolean allowSockets) {
        this.base = base;
        this.allowSockets = allowSockets;
    }

    public void run() {
        while (!stop) {
            FileLock lock = new FileLock(new TraceSystem(null, false), 100);
            try {
                lock.lock(FILE, allowSockets);
                base.trace(lock + " locked");
                locks++;
                if (locks > 1) {
                    System.err.println("ERROR! LOCKS=" + locks + " sockets=" + allowSockets);
                    stop = true;
                }
                Thread.sleep(wait + (int) (Math.random() * wait));
                locks--;
                if ((Math.random() * 50) < KILL) {
                    base.trace(lock + " kill");
                    lock = null;
                    System.gc();
                } else {
                    base.trace(lock + " unlock");
                    lock.unlock();
                }
                if (locks < 0) {
                    System.err.println("ERROR! LOCKS=" + locks);
                    stop = true;
                }
            } catch (Exception e) {
                // log(id+" cannot lock: " + e);
            }
            try {
                Thread.sleep(wait + (int) (Math.random() * wait));
            } catch (InterruptedException e1) {
                // ignore
            }
        }
    }

}
