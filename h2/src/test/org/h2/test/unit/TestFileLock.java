/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.File;

import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.test.TestBase;
import org.h2.util.FileUtils;


/**
 * @author Thomas
 */
public class TestFileLock extends TestBase implements Runnable {

    int wait;
    static final int KILL = 10;
    static final String FILE = BASE_DIR + "/test.lock";

    private boolean allowSockets;
    private static volatile int locks;
    private static volatile boolean stop;

    public TestFileLock() {}

    public void test() throws Exception {
        new File(FILE).delete();
        int threadCount = getSize(3, 5);
        wait = getSize(20, 200);
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(new TestFileLock(this, false));
            threads[i].start();
            Thread.sleep(wait + (int) (Math.random() * wait));
        }
        trace("wait");
        Thread.sleep(100);
        stop = true;
        trace("STOP file");
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        check(locks, 0);
        FileUtils.delete(FILE);
        stop = false;
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(new TestFileLock(this, true));
            threads[i].start();
            Thread.sleep(wait + (int) (Math.random() * wait));
        }
        trace("wait");
        Thread.sleep(100);
        stop = true;
        trace("STOP sockets");
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
            FileLock lock = new FileLock(new TraceSystem(null), 100);
            try {
                lock.lock(FILE, allowSockets);
                base.trace(lock + " locked");
                locks++;
                if (locks > 1) {
                    System.err.println("ERROR! LOCKS=" + locks);
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
                // e.printStackTrace();
            }
            try {
                Thread.sleep(wait + (int) (Math.random() * wait));
            } catch (InterruptedException e1) {
                // ignore
            }
        }
    }

}
