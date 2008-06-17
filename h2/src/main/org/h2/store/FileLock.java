/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.store.fs.FileSystem;
import org.h2.util.ByteUtils;
import org.h2.util.NetUtils;
import org.h2.util.RandomUtils;
import org.h2.util.SortedProperties;

/**
 * The file lock is used to lock a database so that only one process can write
 * to it. It uses a cooperative locking protocol. Usually a .lock.db file is
 * used, but locking by creating a socket is supported as well.
 */
public class FileLock {
    
    /**
     * This locking method means no locking is used at all.
     */
    public static final int LOCK_NO = 0;

    /**
     * This locking method means the cooperative file locking protocol should be
     * used.
     */
    public static final int LOCK_FILE = 1;

    /**
     * This locking method means a socket is created on the given machine.
     */
    public static final int LOCK_SOCKET = 2;
    
    private static final String MAGIC = "FileLock";
    private static final String FILE = "file", SOCKET = "socket";
    private static final int RANDOM_BYTES = 16;
    private static final int SLEEP_GAP = 25;
    private static final int TIME_GRANULARITY = 2000;

    // TODO lock: maybe not so secure! what if tread does not have chance to run?
    // TODO lock: implement locking method using java 1.4 FileLock
    // TODO log / messages: use translatable messages
    // private java.nio.channels.FileLock fileLock;
    
    /**
     * The lock file name.
     */
    volatile String fileName;
    
    /**
     * The server socket (only used when using the SOCKET mode).
     */
    volatile ServerSocket socket;
    
    /**
     * The file system.
     */
    FileSystem fs;
    
    /**
     * The number of milliseconds to sleep after checking a file.
     */
    int sleep;
    
    /**
     * The trace object.
     */
    Trace trace;
    
    /**
     * The last time the lock file was written.
     */
    long lastWrite;

    private String method, ipAddress;
    private Properties properties;
    private boolean locked;

    /**
     * Create a new file locking object.
     * 
     * @param traceSystem the trace system to use
     * @param sleep the number of milliseconds to sleep
     */
    public FileLock(TraceSystem traceSystem,  int sleep) {
        this.trace = traceSystem.getTrace(Trace.FILE_LOCK);
        this.sleep = sleep;
    }

    /**
     * Lock the file if possible. A file may only be locked once.
     * 
     * @param fileName the name of the properties file to use
     * @param allowSocket if the socket locking protocol should be used if
     *            possible
     * @throws SQLException if locking was not successful
     */
    public synchronized void lock(String fileName, boolean allowSocket) throws SQLException {
        this.fs = FileSystem.getInstance(fileName);
        this.fileName = fileName;
        if (locked) {
            throw Message.getInternalError("already locked");
        }
        if (allowSocket) {
            lockSocket();
        } else {
            lockFile();
        }
        locked = true;
    }
    
    /**
     * Unlock the file. The watchdog thread is stopped. This method does nothing
     * if the file is already unlocked.
     */
    public synchronized void unlock() {
        if (!locked) {
            return;
        }
        try {
            if (fileName != null) {
                if (load().equals(properties)) {
                    fs.delete(fileName);
                }
            }
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {
            trace.debug("unlock", e);
        }
        fileName = null;
        socket = null;
        locked = false;
    }

    /**
     * This finalizer unlocks the file if necessary.
     */
    protected void finalize() {
        if (!SysProperties.runFinalize) {
            return;
        }
        if (locked) {
            unlock();
        }
    }

//    void kill() {
//        socket = null;
//        file = null;
//        locked = false;
//        trace("killed", null);
//    }

    /**
     * Save the lock file.
     */
    void save() throws SQLException {
        try {
            OutputStream out = fs.openFileOutputStream(fileName, false);
            try {
                properties.setProperty("method", String.valueOf(method));
                properties.store(out, MAGIC);
            } finally {
                out.close();
            }
            lastWrite = fs.getLastModified(fileName);
            trace.debug("save " + properties);
        } catch (IOException e) {
            throw getException(e);
        }
    }

    private Properties load() throws SQLException {
        try {
            Properties p2 = SortedProperties.loadProperties(fileName);
            trace.debug("load " + p2);
            return p2;
        } catch (IOException e) {
            throw getException(e);
        }
    }

    private void waitUntilOld() throws SQLException {
        for (int i = 0; i < 10; i++) {
            long last = fs.getLastModified(fileName);
            long dist = System.currentTimeMillis() - last;
            if (dist < -TIME_GRANULARITY) {
                throw error("Lock file modified in the future: dist=" + dist);
            }
            if (dist < SLEEP_GAP) {
                try {
                    Thread.sleep(dist + 1);
                } catch (Exception e) {
                    trace.debug("sleep", e);
                }
            } else {
                return;
            }
        }
        throw error("Lock file recently modified");
    }

    private void lockFile() throws SQLException {
        method = FILE;
        properties = new SortedProperties();
        byte[] bytes = RandomUtils.getSecureBytes(RANDOM_BYTES);
        String random = ByteUtils.convertBytesToString(bytes);
        properties.setProperty("id", Long.toHexString(System.currentTimeMillis())+random);
        if (!fs.createNewFile(fileName)) {
            waitUntilOld();
            String m2 = load().getProperty("method", FILE);
            if (!m2.equals(FILE)) {
                throw error("Unsupported lock method " + m2);
            }
            save();
            sleep(2 * sleep);
            if (!load().equals(properties)) {
                throw error("Locked by another process");
            }
            fs.delete(fileName);
            if (!fs.createNewFile(fileName)) {
                throw error("Another process was faster");
            }
        }
        save();
        sleep(SLEEP_GAP);
        if (!load().equals(properties)) {
            fileName = null;
            throw error("Concurrent update");
        }
        Thread watchdog = new Thread(new Runnable() {
            public void run() {
                try {
                    while (fileName != null) {
                        // trace.debug("watchdog check");
                        try {
                            if (!fs.exists(fileName) || fs.getLastModified(fileName) != lastWrite) {
                                save();
                            }
                            Thread.sleep(sleep);
                        } catch (Exception e) {
                            trace.debug("watchdog", e);
                        }
                    }
                } catch (Exception e) {
                    trace.debug("watchdog", e);
                }
                trace.debug("watchdog end");
            }
        });
        watchdog.setName("H2 File Lock Watchdog " + fileName);
        watchdog.setDaemon(true);
        watchdog.setPriority(Thread.MAX_PRIORITY - 1);
        watchdog.start();
    }

    private void lockSocket() throws SQLException {
        method = SOCKET;
        properties = new SortedProperties();
        try {
            // TODO documentation: if this returns 127.0.0.1, 
            // the computer is probably not networked
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw getException(e);
        }
        if (!fs.createNewFile(fileName)) {
            waitUntilOld();
            long read = fs.getLastModified(fileName);
            Properties p2 = load();
            String m2 = p2.getProperty("method", SOCKET);
            if (m2.equals(FILE)) {
                lockFile();
                return;
            } else if (!m2.equals(SOCKET)) {
                throw error("Unsupported lock method " + m2);
            }
            String ip = p2.getProperty("ipAddress", ipAddress);
            if (!ipAddress.equals(ip)) {
                throw error("Locked by another computer: " + ip);
            }
            String port = p2.getProperty("port", "0");
            int portId = Integer.parseInt(port);
            InetAddress address;
            try {
                address = InetAddress.getByName(ip);
            } catch (UnknownHostException e) {
                throw getException(e);
            }
            for (int i = 0; i < 3; i++) {
                try {
                    Socket s = new Socket(address, portId);
                    s.close();
                    throw error("Locked by another process");
                } catch (BindException e) {
                    throw error("Bind Exception");
                } catch (ConnectException e) {
                    trace.debug("lockSocket not connected " + port, e);
                } catch (IOException e) {
                    throw error("IOException");
                }
            }
            if (read != fs.getLastModified(fileName)) {
                throw error("Concurrent update");
            }
            fs.delete(fileName);
            if (!fs.createNewFile(fileName)) {
                throw error("Another process was faster");
            }
        }
        try {
            // 0 to use any free port
            socket = NetUtils.createServerSocket(0, false);
            int port = socket.getLocalPort();
            properties.setProperty("ipAddress", ipAddress);
            properties.setProperty("port", String.valueOf(port));
        } catch (Exception e) {
            trace.debug("lock", e);
            socket = null;
            lockFile();
            return;
        }
        save();
        Thread watchdog = new Thread(new Runnable() {
            public void run() {
                while (socket != null) {
                    try {
                        trace.debug("watchdog accept");
                        Socket s = socket.accept();
                        s.close();
                    } catch (Exception e) {
                        trace.debug("watchdog", e);
                    }
                }
                trace.debug("watchdog end");
            }
        });
        watchdog.setDaemon(true);
        watchdog.setName("H2 File Lock Watchdog (Socket) " + fileName);
        watchdog.start();
    }

    private void sleep(int time) throws SQLException {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw getException(e);
        }
    }

    private SQLException getException(Throwable t) {
        return Message.getSQLException(ErrorCode.ERROR_OPENING_DATABASE, null, t);
    }

    private SQLException error(String reason) {
        return Message.getSQLException(ErrorCode.DATABASE_ALREADY_OPEN_1, reason);
    }

    /**
     * Get the file locking method type given a method name.
     * 
     * @param method the method name
     * @return the method type
     * @throws SQLException if the method name is unknown
     */
    public static int getFileLockMethod(String method) throws SQLException {
        if (method == null || method.equalsIgnoreCase("FILE")) {
            return FileLock.LOCK_FILE;
        } else if (method.equalsIgnoreCase("NO")) {
            return FileLock.LOCK_NO;
        } else if (method.equalsIgnoreCase("SOCKET")) {
            return FileLock.LOCK_SOCKET;
        } else {
            throw Message.getSQLException(ErrorCode.UNSUPPORTED_LOCK_METHOD_1, method);
        }
    }

}
