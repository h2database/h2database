/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.util.ByteUtils;
import org.h2.util.FileUtils;
import org.h2.util.RandomUtils;

/**
 * @author Thomas
 */
public class FileLock {

    public static final int LOCK_NO = 0, LOCK_FILE = 1, LOCK_SOCKET = 2;

    // TODO lock: maybe not so secure! what if tread does not have chance to run?
    // TODO lock: implement locking method using java 1.4 FileLock
    private static final String MAGIC = "FileLock";
    private static final String FILE = "file", SOCKET = "socket";
    private static final int RANDOM_BYTES = 16;
    private static final int SLEEP_GAP = 20;
    private static final int TIME_GRANULARITY = 2000;

    private String method, ipAddress;
    private  int sleep;
    private long lastWrite;
    private Properties properties;
    private volatile String fileName;
    private volatile ServerSocket socket;
    private boolean locked;
    private Trace trace;

    public FileLock(TraceSystem traceSystem,  int sleep) {
        this.trace = traceSystem.getTrace(Trace.FILE_LOCK);
        this.sleep = sleep;
    }

    public synchronized void lock(String fileName, boolean allowSocket) throws SQLException {
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

    protected void finalize() {
        if (!Constants.RUN_FINALIZE) {
            return;
        }
        if(locked) {
            unlock();
        }
    }

//    void kill() {
//        socket = null;
//        file = null;
//        locked = false;
//        trace("killed", null);
//    }

    // TODO log / messages: use translatable messages!
    public synchronized void unlock() {
        if(!locked) {
            return;
        }
        try {
            if (fileName != null) {
                if (load().equals(properties)) {
                    FileUtils.delete(fileName);
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

    void save() throws SQLException {
        try {
            File file = new File(fileName);
            // TODO file: delegate to FileUtils
            FileOutputStream out = FileUtils.openFileOutputStream(file);
            try {
                properties.setProperty("method", String.valueOf(method));
                properties.store(out, MAGIC);
            } finally {
                out.close();
            }
            lastWrite = file.lastModified();
            trace.debug("save " + properties);
        } catch(IOException e) {
            throw getException(e);
        }
    }

    private Properties load() throws SQLException {
        try {
            Properties p2 = FileUtils.loadProperties(new File(fileName));
            trace.debug("load " + p2);
            return p2;
        } catch(IOException e) {
            throw getException(e);
        }
    }

    private void waitUntilOld() throws SQLException {
        File file = new File(fileName);
        for(int i=0; i<10; i++) {
            long last = file.lastModified();
            long dist = System.currentTimeMillis() - last;
            if(dist < -TIME_GRANULARITY) {
                throw error("Lock file modified in the future: dist=" + dist);
            }
            if(dist < SLEEP_GAP) {
                try {
                    Thread.sleep(dist+1);
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
        properties = new Properties();
        byte[] bytes = RandomUtils.getSecureBytes(RANDOM_BYTES);
        String random = ByteUtils.convertBytesToString(bytes);
        properties.setProperty("id", Long.toHexString(System.currentTimeMillis())+random);
        if (!FileUtils.createNewFile(fileName)) {
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
            FileUtils.delete(fileName);
            if (!FileUtils.createNewFile(fileName)) {
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
                    File file = new File(fileName);
                    while (fileName != null) {
                        // trace.debug("watchdog check");
                        try {
                            if (!file.exists() || file.lastModified() != lastWrite) {
                                save();
                            }
                            Thread.sleep(sleep);
                        } catch (Exception e) {
                            trace.debug("watchdog", e);
                        }
                    }
                } catch(Exception e) {
                    trace.debug("watchdog", e);
                }
                trace.debug("watchdog end");
            }
        });
        watchdog.setName("H2 File Lock Watchdog " + fileName);
        watchdog.setDaemon(true);
        watchdog.setPriority(Thread.MAX_PRIORITY-1);
        watchdog.start();
    }

    private void lockSocket() throws SQLException {
        method = SOCKET;
        properties = new Properties();
        try {
            // TODO documentation: if this returns 127.0.0.1, the computer is probably not networked
            ipAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw getException(e);
        }
        if (!FileUtils.createNewFile(fileName)) {
            waitUntilOld();
            File file = new File(fileName);
            long read = file.lastModified();
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
            if (read != file.lastModified()) {
                throw error("Concurrent update");
            }
            FileUtils.delete(fileName);
            if (!FileUtils.createNewFile(fileName)) {
                throw error("Another process was faster");
            }
        }
        try {
            // 0 to use any free port
            socket = new ServerSocket(0);
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
        } catch(InterruptedException e) {
            throw getException(e);
        }
    }

    private SQLException getException(Throwable t) {
        return Message.getSQLException(Message.ERROR_OPENING_DATABASE, null, t);
    }

    private SQLException error(String reason) {
        return Message.getSQLException(Message.DATABASE_ALREADY_OPEN_1, reason);
    }
    
    public static int getFileLockMethod(String method) throws JdbcSQLException {
        if(method == null || method.equalsIgnoreCase("FILE")) {
            return FileLock.LOCK_FILE;
        } else if(method.equalsIgnoreCase("NO")) {
            return FileLock.LOCK_NO;
        } else if(method.equalsIgnoreCase("SOCKET")) {
            return FileLock.LOCK_SOCKET;
        } else {
            throw Message.getSQLException(Message.UNSUPPORTED_LOCK_METHOD_1, method);
        }
    }    

}
