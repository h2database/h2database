/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Sergi Vladykin
 */
package org.h2.test.unit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.h2.build.BuildBase;
import org.h2.test.TestBase;
import org.h2.util.NetUtils;
import org.h2.util.Task;
import org.h2.util.Utils10;

/**
 * Test the network utilities from {@link NetUtils}.
 *
 * @author Sergi Vladykin
 * @author Tomas Pospichal
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
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testFrequentConnections(true, 100);
        testFrequentConnections(false, 1000);
        testIpToShortForm();
        testTcpQuickack();
    }

    private static void testFrequentConnections(boolean ssl, int count) throws Exception {
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
            Set<ConnectWorker> workers = new HashSet<>();
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

        ConnectWorker(boolean ssl, AtomicInteger counter) {
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

    private void testIpToShortForm() throws Exception {
        testIpToShortForm("1.2.3.4", "1.2.3.4");
        testIpToShortForm("1:2:3:4:a:b:c:d", "1:2:3:4:a:b:c:d");
        testIpToShortForm("::1", "::1");
        testIpToShortForm("1::", "1::");
        testIpToShortForm("c1c1:0:0:2::fffe", "c1c1:0:0:2:0:0:0:fffe");
    }

    private void testIpToShortForm(String expected, String source) throws Exception {
        byte[] addr = InetAddress.getByName(source).getAddress();
        testIpToShortForm(expected, addr, false);
        if (expected.indexOf(':') >= 0) {
            expected = '[' + expected + ']';
        }
        testIpToShortForm(expected, addr, true);
    }

    private void testIpToShortForm(String expected, byte[] addr, boolean addBrackets) {
        assertEquals(expected, NetUtils.ipToShortForm(null, addr, addBrackets).toString());
        assertEquals(expected, NetUtils.ipToShortForm(new StringBuilder(), addr, addBrackets).toString());
        assertEquals(expected,
                NetUtils.ipToShortForm(new StringBuilder("*"), addr, addBrackets).deleteCharAt(0).toString());
    }

    private void testTcpQuickack() {
        final boolean ssl = !config.ci && BuildBase.getJavaVersion() < 11;
        try (ServerSocket serverSocket = NetUtils.createServerSocket(PORT, ssl)) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    try (Socket s = NetUtils.createLoopbackSocket(PORT, ssl)) {
                        s.getInputStream().read();
                    } catch (IOException e) {
                    }
                }
            };
            thread.start();
            try (Socket socket = serverSocket.accept()) {
                boolean supported = Utils10.setTcpQuickack(socket, true);
                if (supported) {
                    assertTrue(Utils10.getTcpQuickack(socket));
                    Utils10.setTcpQuickack(socket, false);
                    assertFalse(Utils10.getTcpQuickack(socket));
                }
                socket.getOutputStream().write(1);
            } finally {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
