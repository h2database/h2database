/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.pg;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.server.Service;
import org.h2.util.NetUtils;
import org.h2.util.Tool;
import org.h2.util.Utils;
import org.h2.util.Utils10;
import org.h2.util.Utils21;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * This class implements a subset of the PostgreSQL protocol as described here:
 * https://www.postgresql.org/docs/devel/protocol.html
 * The PostgreSQL catalog is described here:
 * https://www.postgresql.org/docs/7.4/catalogs.html
 *
 * @author Thomas Mueller
 * @author Sergi Vladykin 2009-07-03 (convertType)
 */
public class PgServer implements Service {

    /**
     * The default port to use for the PG server.
     * This value is also in the documentation and in the Server javadoc.
     */
    public static final int DEFAULT_PORT = 5435;

    /**
     * The VARCHAR type.
     */
    public static final int PG_TYPE_VARCHAR = 1043;

    public static final int PG_TYPE_BOOL = 16;
    public static final int PG_TYPE_BYTEA = 17;
    public static final int PG_TYPE_BPCHAR = 1042;
    public static final int PG_TYPE_INT8 = 20;
    public static final int PG_TYPE_INT2 = 21;
    public static final int PG_TYPE_INT4 = 23;
    public static final int PG_TYPE_TEXT = 25;
    public static final int PG_TYPE_FLOAT4 = 700;
    public static final int PG_TYPE_FLOAT8 = 701;
    public static final int PG_TYPE_UNKNOWN = 705;
    public static final int PG_TYPE_INT2_ARRAY = 1005;
    public static final int PG_TYPE_INT4_ARRAY = 1007;
    public static final int PG_TYPE_VARCHAR_ARRAY = 1015;
    public static final int PG_TYPE_DATE = 1082;
    public static final int PG_TYPE_TIME = 1083;
    public static final int PG_TYPE_TIMETZ = 1266;
    public static final int PG_TYPE_TIMESTAMP = 1114;
    public static final int PG_TYPE_TIMESTAMPTZ = 1184;
    public static final int PG_TYPE_NUMERIC = 1700;

    private final HashSet<Integer> typeSet = new HashSet<>();

    private int port = PgServer.DEFAULT_PORT;
    private boolean portIsSet;
    private boolean stop;
    private boolean trace;
    private ServerSocket serverSocket;
    private final Set<PgServerThread> running = Collections.
            synchronizedSet(new HashSet<PgServerThread>());
    private final AtomicInteger pid = new AtomicInteger();
    private String baseDir;
    private boolean allowOthers;
    private boolean isDaemon;
    private boolean ifExists = true;
    private boolean virtualThreads;
    private String key, keyDatabase;

    @Override
    public void init(String... args) {
        port = DEFAULT_PORT;
        for (int i = 0; args != null && i < args.length; i++) {
            String a = args[i];
            if (Tool.isOption(a, "-trace")) {
                trace = true;
            } else if (Tool.isOption(a, "-pgPort")) {
                port = Integer.decode(args[++i]);
                portIsSet = true;
            } else if (Tool.isOption(a, "-baseDir")) {
                baseDir = args[++i];
            } else if (Tool.isOption(a, "-pgAllowOthers")) {
                allowOthers = true;
            } else if (Tool.isOption(a, "-pgDaemon")) {
                isDaemon = true;
            } else if (Tool.isOption(a,  "-pgVirtualThreads")) {
                virtualThreads = Utils.parseBoolean(args[++i], virtualThreads, true);
            } else if (Tool.isOption(a, "-ifExists")) {
                ifExists = true;
            } else if (Tool.isOption(a, "-ifNotExists")) {
                ifExists = false;
            } else if (Tool.isOption(a, "-key")) {
                key = args[++i];
                keyDatabase = args[++i];
            }
        }
        // int testing;
        // trace = true;
    }

    boolean getTrace() {
        return trace;
    }

    /**
     * Print a message if the trace flag is enabled.
     *
     * @param s the message
     */
    void trace(String s) {
        if (trace) {
            System.out.println(s);
        }
    }

    /**
     * Remove a thread from the list.
     *
     * @param t the thread to remove
     */
    synchronized void remove(PgServerThread t) {
        running.remove(t);
    }

    /**
     * Print the stack trace if the trace flag is enabled.
     *
     * @param e the exception
     */
    void traceError(Exception e) {
        if (trace) {
            e.printStackTrace();
        }
    }

    @Override
    public String getURL() {
        return "pg://" + NetUtils.getLocalAddress() + ":" + port;
    }

    @Override
    public int getPort() {
        return port;
    }

    private boolean allow(Socket socket) {
        if (allowOthers) {
            return true;
        }
        try {
            return NetUtils.isLocalAddress(socket);
        } catch (UnknownHostException e) {
            traceError(e);
            return false;
        }
    }

    @Override
    public void start() {
        stop = false;
        try {
            serverSocket = NetUtils.createServerSocket(port, false);
        } catch (DbException e) {
            if (!portIsSet) {
                serverSocket = NetUtils.createServerSocket(0, false);
            } else {
                throw e;
            }
        }
        port = serverSocket.getLocalPort();
    }

    @Override
    public void listen() {
        String threadName = Thread.currentThread().getName();
        try {
            while (!stop) {
                Socket s = serverSocket.accept();
                if (!allow(s)) {
                    trace("Connection not allowed");
                    s.close();
                } else {
                    Utils10.setTcpQuickack(s, true);
                    PgServerThread c = new PgServerThread(s, this);
                    running.add(c);
                    int id = pid.incrementAndGet();
                    c.setProcessId(id);
                    Thread thread;
                    if (virtualThreads) {
                        thread = Utils21.newVirtualThread(c);
                    } else {
                        thread = new Thread(c);
                        thread.setDaemon(isDaemon);
                    }
                    thread.setName(threadName + " thread-" + id);
                    c.setThread(thread);
                    thread.start();
                }
            }
        } catch (Exception e) {
            if (!stop) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
        // TODO server: combine with tcp server
        if (!stop) {
            stop = true;
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    // TODO log exception
                    e.printStackTrace();
                }
                serverSocket = null;
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        for (PgServerThread c : new ArrayList<>(running)) {
            c.close();
            try {
                Thread t = c.getThread();
                if (t != null) {
                    t.join(100);
                }
            } catch (Exception e) {
                // TODO log exception
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean isRunning(boolean traceError) {
        if (serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(serverSocket.getLocalPort(), false);
            s.close();
            return true;
        } catch (Exception e) {
            if (traceError) {
                traceError(e);
            }
            return false;
        }
    }

    /**
     * Get the thread with the given process id.
     *
     * @param processId the process id
     * @return the thread
     */
    PgServerThread getThread(int processId) {
        for (PgServerThread c : new ArrayList<>(running)) {
            if (c.getProcessId() == processId) {
                return c;
            }
        }
        return null;
    }

    String getBaseDir() {
        return baseDir;
    }

    @Override
    public boolean getAllowOthers() {
        return allowOthers;
    }

    @Override
    public String getType() {
        return "PG";
    }

    @Override
    public String getName() {
        return "H2 PG Server";
    }

    boolean getIfExists() {
        return ifExists;
    }

    /**
     * Returns the name of the given type.
     *
     * @param pgType the PostgreSQL type oid
     * @return the name of the given type
     */
    public static String formatType(int pgType) {
        int valueType;
        switch (pgType) {
        case 0:
            return "-";
        case PG_TYPE_BOOL:
            valueType = Value.BOOLEAN;
            break;
        case PG_TYPE_BYTEA:
            valueType = Value.VARBINARY;
            break;
        case 18:
            return "char";
        case 19:
            return "name";
        case PG_TYPE_INT8:
            valueType = Value.BIGINT;
            break;
        case PG_TYPE_INT2:
            valueType = Value.SMALLINT;
            break;
        case 22:
            return "int2vector";
        case PG_TYPE_INT4:
            valueType = Value.INTEGER;
            break;
        case 24:
            return "regproc";
        case PG_TYPE_TEXT:
            valueType = Value.CLOB;
            break;
        case PG_TYPE_FLOAT4:
            valueType = Value.REAL;
            break;
        case PG_TYPE_FLOAT8:
            valueType = Value.DOUBLE;
            break;
        case PG_TYPE_INT2_ARRAY:
            return "smallint[]";
        case PG_TYPE_INT4_ARRAY:
            return "integer[]";
        case PG_TYPE_VARCHAR_ARRAY:
            return "character varying[]";
        case PG_TYPE_BPCHAR:
            valueType = Value.CHAR;
            break;
        case PG_TYPE_VARCHAR:
            valueType = Value.VARCHAR;
            break;
        case PG_TYPE_DATE:
            valueType = Value.DATE;
            break;
        case PG_TYPE_TIME:
            valueType = Value.TIME;
            break;
        case PG_TYPE_TIMETZ:
            valueType = Value.TIME_TZ;
            break;
        case PG_TYPE_TIMESTAMP:
            valueType = Value.TIMESTAMP;
            break;
        case PG_TYPE_TIMESTAMPTZ:
            valueType = Value.TIMESTAMP_TZ;
            break;
        case PG_TYPE_NUMERIC:
            valueType = Value.NUMERIC;
            break;
        case 2205:
            return "regclass";
        default:
            return "???";
        }
        return Value.getTypeName(valueType);
    }

    /**
     * Convert the SQL type to a PostgreSQL type
     *
     * @param type the SQL type
     * @return the PostgreSQL type
     */
    public static int convertType(TypeInfo type) {
        switch (type.getValueType()) {
        case Value.BOOLEAN:
            return PG_TYPE_BOOL;
        case Value.VARCHAR:
            return PG_TYPE_VARCHAR;
        case Value.NULL:
        case Value.CLOB:
            return PG_TYPE_TEXT;
        case Value.CHAR:
            return PG_TYPE_BPCHAR;
        case Value.SMALLINT:
            return PG_TYPE_INT2;
        case Value.INTEGER:
            return PG_TYPE_INT4;
        case Value.BIGINT:
            return PG_TYPE_INT8;
        case Value.NUMERIC:
        case Value.DECFLOAT:
            return PG_TYPE_NUMERIC;
        case Value.REAL:
            return PG_TYPE_FLOAT4;
        case Value.DOUBLE:
            return PG_TYPE_FLOAT8;
        case Value.TIME:
            return PG_TYPE_TIME;
        case Value.TIME_TZ:
            return PG_TYPE_TIMETZ;
        case Value.DATE:
            return PG_TYPE_DATE;
        case Value.TIMESTAMP:
            return PG_TYPE_TIMESTAMP;
        case Value.TIMESTAMP_TZ:
            return PG_TYPE_TIMESTAMPTZ;
        case Value.BINARY:
        case Value.VARBINARY:
            return PG_TYPE_BYTEA;
        case Value.ARRAY: {
            type = (TypeInfo) type.getExtTypeInfo();
            switch (type.getValueType()) {
            case Value.SMALLINT:
                return PG_TYPE_INT2_ARRAY;
            case Value.INTEGER:
                return PG_TYPE_INT4_ARRAY;
            case Value.VARCHAR:
                return PG_TYPE_VARCHAR_ARRAY;
            default:
                return PG_TYPE_VARCHAR_ARRAY;
            }
        }
        default:
            return PG_TYPE_UNKNOWN;
        }
    }

    /**
     * Get the type hash set.
     *
     * @return the type set
     */
    HashSet<Integer> getTypeSet() {
        return typeSet;
    }

    /**
     * Check whether a data type is supported.
     * A warning is logged if not.
     *
     * @param type the type
     */
    void checkType(int type) {
        if (!typeSet.contains(type)) {
            trace("Unsupported type: " + type);
        }
    }

    /**
     * If no key is set, return the original database name. If a key is set,
     * check if the key matches. If yes, return the correct database name. If
     * not, throw an exception.
     *
     * @param db the key to test (or database name if no key is used)
     * @return the database name
     * @throws DbException if a key is set but doesn't match
     */
    public String checkKeyAndGetDatabaseName(String db) {
        if (key == null) {
            return db;
        }
        if (key.equals(db)) {
            return keyDatabase;
        }
        throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
    }

    @Override
    public boolean isDaemon() {
        return isDaemon;
    }

}
