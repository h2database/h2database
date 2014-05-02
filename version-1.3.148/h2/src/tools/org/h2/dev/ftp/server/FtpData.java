/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.ftp.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.h2.store.fs.FileSystem;
import org.h2.util.IOUtils;

/**
 * The implementation of the data channel of the FTP server.
 */
public class FtpData extends Thread {

    private FtpServer server;
    private InetAddress address;
    private ServerSocket serverSocket;
    private volatile Socket socket;
    private boolean active;
    private int port;

    FtpData(FtpServer server, InetAddress address, ServerSocket serverSocket) {
        this.server = server;
        this.address = address;
        this.serverSocket = serverSocket;
    }

    FtpData(FtpServer server, InetAddress address, int port) {
        this.server = server;
        this.address = address;
        this.port = port;
        active = true;
    }

    public void run() {
        try {
            synchronized (this) {
                Socket s = serverSocket.accept();
                if (s.getInetAddress().equals(address)) {
                    server.trace("Data connected:" + s.getInetAddress() + " expected:" + address);
                    socket = s;
                    notifyAll();
                } else {
                    server.trace("Data REJECTED:" + s.getInetAddress() + " expected:" + address);
                    close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void connect() throws IOException {
        if (active) {
            socket = new Socket(address, port);
        } else {
            waitUntilConnected();
        }
    }

    private void waitUntilConnected() {
        while (serverSocket != null && socket == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        server.trace("connected");
    }

    /**
     * Close the socket.
     */
    void close() {
        serverSocket = null;
        socket = null;
    }

    /**
     * Read a file from a client.
     *
     * @param fs the target file system
     * @param fileName the target file name
     */
    synchronized void receive(FileSystem fs, String fileName) throws IOException {
        connect();
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = fs.openFileOutputStream(fileName, false);
            IOUtils.copy(in, out);
            out.close();
        } finally {
            socket.close();
        }
        server.trace("closed");
    }

    /**
     * Send a file to the client. This method waits until the client has
     * connected.
     *
     * @param fs the source file system
     * @param fileName the source file name
     * @param skip the number of bytes to skip
     */
    synchronized void send(FileSystem fs, String fileName, long skip) throws IOException {
        connect();
        try {
            OutputStream out = socket.getOutputStream();
            InputStream in = fs.openFileInputStream(fileName);
            IOUtils.skipFully(in, skip);
            IOUtils.copy(in, out);
            in.close();
        } finally {
            socket.close();
        }
        server.trace("closed");
    }

    /**
     * Wait until the client has connected, and then send the data to him.
     *
     * @param data the data to send
     */
    synchronized void send(byte[] data) throws IOException {
        connect();
        try {
            OutputStream out = socket.getOutputStream();
            out.write(data);
        } finally {
            socket.close();
        }
        server.trace("closed");
    }

}
