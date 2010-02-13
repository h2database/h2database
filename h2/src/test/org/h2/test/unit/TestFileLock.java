/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;
import org.h2.engine.Constants;
import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.test.TestBase;

/**
 * Tests the database file locking facility.
 * Both lock files and sockets locking is tested.
 */
public class TestFileLock extends TestBase implements Runnable {

    private static final String FILE = baseDir + "/test.lock";
    private static volatile int locks;
    private static volatile boolean stop;
    private TestBase base;
    private int wait;
    private boolean allowSockets;

    public TestFileLock() {
        // nothing to do
    }

    TestFileLock(TestBase base, boolean allowSockets) {
        this.base = base;
        this.allowSockets = allowSockets;
    }

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testFutureModificationDate();
        testSimple();
        test(false);
        test(true);
    }

    private void testFutureModificationDate() throws Exception {
        File f = new File(FILE);
        f.delete();
        f.createNewFile();
        f.setLastModified(System.currentTimeMillis() + 10000);
        FileLock lock = new FileLock(new TraceSystem(null), FILE, Constants.LOCK_SLEEP);
        lock.lock(FileLock.LOCK_FILE);
        lock.unlock();
    }

    private void testSimple() {
        FileLock lock1 = new FileLock(new TraceSystem(null), FILE, Constants.LOCK_SLEEP);
        FileLock lock2 = new FileLock(new TraceSystem(null), FILE, Constants.LOCK_SLEEP);
        lock1.lock(FileLock.LOCK_FILE);
        try {
            lock2.lock(FileLock.LOCK_FILE);
            fail();
        } catch (Exception e) {
            // expected
        }
        lock1.unlock();
        lock2 = new FileLock(new TraceSystem(null), FILE, Constants.LOCK_SLEEP);
        lock2.lock(FileLock.LOCK_FILE);
        lock2.unlock();
    }

    private void test(boolean allowSocketsLock) throws Exception {
        int threadCount = getSize(3, 5);
        wait = getSize(20, 200);
        Thread[] threads = new Thread[threadCount];
        new File(FILE).delete();
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(new TestFileLock(this, allowSocketsLock));
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
        assertEquals(0, locks);
    }

    public void run() {
        FileLock lock = null;
        while (!stop) {
            lock = new FileLock(new TraceSystem(null), FILE, 100);
            try {
                lock.lock(allowSockets ? FileLock.LOCK_SOCKET : FileLock.LOCK_FILE);
                base.trace(lock + " locked");
                locks++;
                if (locks > 1) {
                    System.err.println("ERROR! LOCKS=" + locks + " sockets=" + allowSockets);
                    stop = true;
                }
                Thread.sleep(wait + (int) (Math.random() * wait));
                locks--;
                base.trace(lock + " unlock");
                lock.unlock();
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
        if (lock != null) {
            lock.unlock();
        }
    }

}
