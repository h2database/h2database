/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.h2.test.TestBase;
import org.h2.util.Task;
import org.h2.value.Transfer;

/**
 * Test that {@link Transfer} does not allocate memory from a length prefix
 * before the data behind it has been received.
 */
public class TestTransfer extends TestBase {

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
        testOversizedBytesLength();
        testOversizedStringLength();
    }

    private void testOversizedBytesLength() throws Exception {
        // length prefix claims almost 2 GiB, but only three bytes follow
        byte[] payload = { 1, 2, 3 };
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Task client = sendThenClose(server.getLocalPort(), Integer.MAX_VALUE, payload);
            try (Socket socket = server.accept()) {
                Transfer transfer = new Transfer(null, socket);
                transfer.init();
                assertThrows(EOFException.class, () -> transfer.readBytes());
            } finally {
                client.join();
            }
        }
    }

    private void testOversizedStringLength() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Task client = sendThenClose(server.getLocalPort(), Integer.MAX_VALUE, new byte[0]);
            try (Socket socket = server.accept()) {
                Transfer transfer = new Transfer(null, socket);
                transfer.init();
                assertThrows(EOFException.class, () -> transfer.readString());
            } finally {
                client.join();
            }
        }
    }

    private static Task sendThenClose(int port, int length, byte[] payload) {
        return new Task() {
            @Override
            public void call() throws Exception {
                try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeInt(length);
                    out.write(payload);
                    out.flush();
                }
            }
        }.execute();
    }

}
