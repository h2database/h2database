/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Sergi Vladykin
 */
package org.h2.test.unit;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.test.TestBase;
import org.h2.util.NetUtils;
import org.h2.util.Task;

/**
 * Test the network utilities.
 *
 * @author Sergi Vladykin
 */
public class TestNetUtils extends TestBase {

    private static final int WORKER_COUNT = 10;
    private static final int PORT = 9111;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testFrequentConnections(true, 100);
        testFrequentConnections(false, 1000);
    }

    private void testFrequentConnections(boolean ssl, int count) throws Exception {
        final ServerSocket serverSocket = NetUtils.createServerSocket(PORT, ssl);
        final AtomicInteger counter = new AtomicInteger(count);
        Task serverThread = new Task() {
            @Override
            public void call() {
                while (!stop) {
                    try {
                        Socket socket = serverSocket.accept();
                        // System.out.println("opened " + counter);
                        socket.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                // System.out.println("stopped ");

            }
        };
        serverThread.execute();
        try {
            Set<ConnectWorker> workers = new HashSet<ConnectWorker>();
            for (int i = 0; i < WORKER_COUNT; i++) {
                workers.add(new ConnectWorker(ssl, counter));
            }
            // ensure the server is started
            Thread.sleep(100);
            for (ConnectWorker worker : workers) {
                worker.start();
            }
            for (ConnectWorker worker : workers) {
                worker.join();
                Exception e = worker.getException();
                if (e != null) {
                    e.printStackTrace();
                }
            }
        } finally {
            try {
                serverSocket.close();
            } catch (Exception e) {
                // ignore
            }
            serverThread.get();
        }
    }

    /**
     * A worker thread to test connecting.
     */
    private static class ConnectWorker extends Thread {

        private final boolean ssl;
        private final AtomicInteger counter;
        private Exception exception;

        public ConnectWorker(boolean ssl, AtomicInteger counter) {
            this.ssl = ssl;
            this.counter = counter;
        }

        @Override
        public void run() {
            try {
                while (!isInterrupted() && counter.decrementAndGet() > 0) {
                    Socket socket = NetUtils.createLoopbackSocket(PORT, ssl);
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            } catch (Exception e) {
                exception = new Exception("count: " + counter, e);
            }
        }

        public Exception getException() {
            return exception;
        }

    }

}
