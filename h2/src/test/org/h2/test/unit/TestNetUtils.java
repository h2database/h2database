/**
 * 
 */
package org.h2.test.unit;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.engine.Constants;
import org.h2.test.TestBase;
import org.h2.util.NetUtils;

/**
 * @author Sergi Vladykin
 */
public class TestNetUtils extends TestBase {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        TestBase.createCaller().init().test();

    }

    @Override
    public void test() throws Exception {
        testFrequentConnections(false);
        testFrequentConnections(true);
    }

    private void testFrequentConnections(boolean ssl) throws Exception {
        final ServerSocket serverSock = NetUtils.createServerSocket(Constants.DEFAULT_TCP_PORT, ssl);
        Thread serverThread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        Socket socket = serverSock.accept();
                        socket.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        };
        serverThread.start();
        // System.out.println("Server started.");
        AtomicInteger counter = new AtomicInteger();
        try {
            Set<ConnectWorker> workers = new HashSet<ConnectWorker>();
            for (int i = 0; i < 10; i++) {
                workers.add(new ConnectWorker(ssl, workers, counter));
            }
            for (ConnectWorker worker : workers) {
                worker.start();
            }
            // System.out.println("Workers started.");
            Exception exception = null;
            for (ConnectWorker worker : workers) {
                worker.join();
                if (exception == null) {
                    exception = worker.getException();
                    // if (exception != null) {
                    // System.out.println("Exception set.");
                    // }
                }
            }
            // System.out.println("All joined.");
            if (exception != null) {
                throw exception;
            }
        } finally {
            serverThread.interrupt();
            try {
                serverSock.close();
            } catch (Exception e) {
                // ignore
            }
            // System.out.println("Server stopped.");
        }
    }

    /**
     * 
     */
    private class ConnectWorker extends Thread {

        private static final int MAX_CONNECT_COUNT = 10000;

        private final boolean ssl;
        private final Set<ConnectWorker> workers;
        private final AtomicInteger counter;

        private volatile Exception exception;

        public ConnectWorker(boolean ssl, Set<ConnectWorker> workers, AtomicInteger counter) {
            this.ssl = ssl;
            this.workers = workers;
            this.counter = counter;
        }

        @Override
        public void run() {
            try {
                while (!isInterrupted() && counter.incrementAndGet() < MAX_CONNECT_COUNT) {
                    Socket sock = NetUtils.createSocket("127.0.0.1", Constants.DEFAULT_TCP_PORT, ssl);
                    // System.out.println(COUNTER.get());
                    try {
                        sock.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            } catch (Exception e) {
                this.exception = e;
                for (ConnectWorker worker : workers) {
                    worker.interrupt();
                }
            }
        }

        /**
         * @return the exception
         */
        public Exception getException() {
            return exception;
        }
    }

}
