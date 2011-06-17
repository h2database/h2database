/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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
import org.h2.engine.Constants;
import org.h2.engine.SessionRemote;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.store.fs.FileSystem;
import org.h2.util.ByteUtils;
import org.h2.util.NetUtils;
import org.h2.util.RandomUtils;
import org.h2.util.SortedProperties;
import org.h2.value.Transfer;

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

    /**
     * This locking method means multiple writers are allowed, and they
     * synchronize themselves.
     */
    public static final int LOCK_SERIALIZED = 3;

    private static final String MAGIC = "FileLock";
    private static final String FILE = "file", SOCKET = "socket", SERIALIZED = "serialized";
    private static final int RANDOM_BYTES = 16;
    private static final int SLEEP_GAP = 25;
    private static final int TIME_GRANULARITY = 2000;

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
    private String uniqueId;
    private Thread watchdog;

    /**
     * Create a new file locking object.
     *
     * @param traceSystem the trace system to use
     * @param sleep the number of milliseconds to sleep
     */
    public FileLock(TraceSystem traceSystem, String fileName, int sleep) {
        this.trace = traceSystem.getTrace(Trace.FILE_LOCK);
        this.fileName = fileName;
        this.sleep = sleep;
    }

    /**
     * Lock the file if possible. A file may only be locked once.
     *
     * @param fileLockMethod the file locking method to use
     * @throws SQLException if locking was not successful
     */
    public synchronized void lock(int fileLockMethod) throws SQLException {
        this.fs = FileSystem.getInstance(fileName);
        checkServer();
        if (locked) {
            Message.throwInternalError("already locked");
        }
        switch (fileLockMethod) {
        case LOCK_FILE:
            lockFile();
            break;
        case LOCK_SOCKET:
            lockSocket();
            break;
        case LOCK_SERIALIZED:
            lockSerialized();
            break;
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
     * Add or change a setting to the properties. This call does not save the
     * file.
     *
     * @param key the key
     * @param value the value
     */
    public void setProperty(String key, String value) {
        if (value == null) {
            properties.remove(key);
        } else {
            properties.put(key, value);
        }
    }

    /**
     * Save the lock file.
     *
     * @return the saved properties
     */
    public Properties save() throws SQLException {
        try {
            OutputStream out = fs.openFileOutputStream(fileName, false);
            try {
                properties.store(out, MAGIC);
            } finally {
                out.close();
            }
            lastWrite = fs.getLastModified(fileName);
            if (trace.isDebugEnabled()) {
                trace.debug("save " + properties);
            }
            return properties;
        } catch (IOException e) {
            throw getExceptionFatal("Could not save properties " + fileName, e);
        }
    }

    private void checkServer() throws SQLException {
        Properties prop = load();
        String server = prop.getProperty("server");
        if (server == null) {
            return;
        }
        boolean running = false;
        String id = prop.getProperty("id");
        try {
            Socket socket = NetUtils.createSocket(server, Constants.DEFAULT_SERVER_PORT, false);
            Transfer transfer = new Transfer(null);
            transfer.setSocket(socket);
            transfer.init();
            transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_6);
            transfer.writeInt(Constants.TCP_PROTOCOL_VERSION_6);
            transfer.writeString(null);
            transfer.writeString(null);
            transfer.writeString(id);
            transfer.writeInt(SessionRemote.SESSION_CHECK_KEY);
            transfer.flush();
            int state = transfer.readInt();
            if (state == SessionRemote.STATUS_OK) {
                running = true;
            }
            transfer.close();
            socket.close();
        } catch (IOException e) {
            return;
        }
        if (running) {
            String payload = server + "/" + id;
            JdbcSQLException ex = Message.getSQLException(ErrorCode.DATABASE_ALREADY_OPEN_1, "Server is running");
            ex.setPayload(payload);
            throw ex;
        }
    }

    /**
     * Load the properties file.
     *
     * @return the properties
     */
    public Properties load() throws SQLException {
        try {
            Properties p2 = SortedProperties.loadProperties(fileName);
            if (trace.isDebugEnabled()) {
                trace.debug("load " + p2);
            }
            return p2;
        } catch (IOException e) {
            throw getExceptionFatal("Could not load properties " + fileName, e);
        }
    }

    private void waitUntilOld() throws SQLException {
        for (int i = 0; i < TIME_GRANULARITY / SLEEP_GAP; i++) {
            long last = fs.getLastModified(fileName);
            long dist = System.currentTimeMillis() - last;
            if (dist < -TIME_GRANULARITY) {
                throw getExceptionFatal("Lock file modification time is in the future; " +
                        "if required please delete the file " + fileName + " manually and retry; " +
                        "dist=" + dist, null);
            } else if (dist > TIME_GRANULARITY) {
                return;
            }
            try {
                Thread.sleep(SLEEP_GAP);
            } catch (Exception e) {
                trace.debug("sleep", e);
            }
        }
        throw getExceptionFatal("Lock file recently modified", null);
    }

    private void setUniqueId() {
        byte[] bytes = RandomUtils.getSecureBytes(RANDOM_BYTES);
        String random = ByteUtils.convertBytesToString(bytes);
        uniqueId = Long.toHexString(System.currentTimeMillis()) + random;
        properties.setProperty("id", uniqueId);
    }

    private void lockSerialized() throws SQLException {
        method = SERIALIZED;
        if (fs.createNewFile(fileName)) {
            properties = new SortedProperties();
            properties.setProperty("method", String.valueOf(method));
            setUniqueId();
            save();
        } else {
            while (true) {
                try {
                    properties = load();
                } catch (SQLException e) {
                    // ignore
                }
                return;
            }
        }
    }

    private void lockFile() throws SQLException {
        method = FILE;
        properties = new SortedProperties();
        properties.setProperty("method", String.valueOf(method));
        setUniqueId();
        if (!fs.createNewFile(fileName)) {
            waitUntilOld();
            String m2 = load().getProperty("method", FILE);
            if (!m2.equals(FILE)) {
                throw getExceptionFatal("Unsupported lock method " + m2, null);
            }
            save();
            sleep(2 * sleep);
            if (!load().equals(properties)) {
                throw getExceptionAlreadyInUse("Locked by another process");
            }
            fs.delete(fileName);
            if (!fs.createNewFile(fileName)) {
                throw getExceptionFatal("Another process was faster", null);
            }
        }
        save();
        sleep(SLEEP_GAP);
        if (!load().equals(properties)) {
            fileName = null;
            throw getExceptionFatal("Concurrent update", null);
        }
        watchdog = new Thread(new Runnable() {
            public void run() {
                try {
                    while (fileName != null) {
                        // trace.debug("watchdog check");
                        try {
                            if (!fs.exists(fileName) || fs.getLastModified(fileName) != lastWrite) {
                                save();
                            }
                            Thread.sleep(sleep);
                        } catch (OutOfMemoryError e) {
                            // ignore
                        } catch (Exception e) {
                            trace.debug("watchdog", e);
                        }
                    }
                } catch (Exception e) {
                    trace.debug("watchdog", e);
                }
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
        properties.setProperty("method", String.valueOf(method));
        setUniqueId();
        // if this returns 127.0.0.1,
        // the computer is probably not networked
        ipAddress = NetUtils.getLocalAddress();
        if (!fs.createNewFile(fileName)) {
            waitUntilOld();
            long read = fs.getLastModified(fileName);
            Properties p2 = load();
            String m2 = p2.getProperty("method", SOCKET);
            if (m2.equals(FILE)) {
                lockFile();
                return;
            } else if (!m2.equals(SOCKET)) {
                throw getExceptionFatal("Unsupported lock method " + m2, null);
            }
            String ip = p2.getProperty("ipAddress", ipAddress);
            if (!ipAddress.equals(ip)) {
                throw getExceptionAlreadyInUse("Locked by another computer: " + ip);
            }
            String port = p2.getProperty("port", "0");
            int portId = Integer.parseInt(port);
            InetAddress address;
            try {
                address = InetAddress.getByName(ip);
            } catch (UnknownHostException e) {
                throw getExceptionFatal("Unknown host " + ip, e);
            }
            for (int i = 0; i < 3; i++) {
                try {
                    Socket s = new Socket(address, portId);
                    s.close();
                    throw getExceptionAlreadyInUse("Locked by another process");
                } catch (BindException e) {
                    throw getExceptionFatal("Bind Exception", null);
                } catch (ConnectException e) {
                    trace.debug("lockSocket not connected " + port, e);
                } catch (IOException e) {
                    throw getExceptionFatal("IOException", null);
                }
            }
            if (read != fs.getLastModified(fileName)) {
                throw getExceptionFatal("Concurrent update", null);
            }
            fs.delete(fileName);
            if (!fs.createNewFile(fileName)) {
                throw getExceptionFatal("Another process was faster", null);
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
        watchdog = new Thread(new Runnable() {
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
            throw getExceptionFatal("Sleep interrupted", e);
        }
    }

    private SQLException getExceptionFatal(String reason, Throwable t) {
        return Message.getSQLException(ErrorCode.ERROR_OPENING_DATABASE_1, t, reason);
    }

    private SQLException getExceptionAlreadyInUse(String reason) {
        JdbcSQLException ex = Message.getSQLException(ErrorCode.DATABASE_ALREADY_OPEN_1, reason);
        String payload = null;
        if (fileName != null) {
            try {
                Properties prop = load();
                payload = prop.getProperty("server") + "/" + prop.getProperty("id");
            } catch (SQLException e) {
                // ignore
            }
        }
        ex.setPayload(payload);
        return ex;
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
        } else if (method.equalsIgnoreCase("SERIALIZED")) {
            return FileLock.LOCK_SERIALIZED;
        } else {
            throw Message.getSQLException(ErrorCode.UNSUPPORTED_LOCK_METHOD_1, method);
        }
    }

    public String getUniqueId() {
        return uniqueId;
    }

}
