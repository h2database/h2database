/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jmx;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.h2.command.Command;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.table.Table;
import org.h2.util.NetworkConnectionInfo;

/**
 * The MBean implementation.
 *
 * @author Eric Dong
 * @author Thomas Mueller
 */
public class DatabaseInfo implements DatabaseInfoMBean {

    private static final Map<String, ObjectName> MBEANS = new HashMap<>();

    /** Database. */
    private final Database database;

    private DatabaseInfo(Database database) {
        if (database == null) {
            throw new IllegalArgumentException("Argument 'database' must not be null");
        }
        this.database = database;
    }

    /**
     * Returns a JMX new ObjectName instance.
     *
     * @param name name of the MBean
     * @param path the path
     * @return a new ObjectName instance
     * @throws JMException if the ObjectName could not be created
     */
    private static ObjectName getObjectName(String name, String path)
            throws JMException {
        name = name.replace(':', '_');
        path = path.replace(':', '_');
        Hashtable<String, String> map = new Hashtable<>();
        map.put("name", name);
        map.put("path", path);
        return new ObjectName("org.h2", map);
    }

    /**
     * Registers an MBean for the database.
     *
     * @param connectionInfo connection info
     * @param database database
     * @throws JMException on failure
     */
    public static void registerMBean(ConnectionInfo connectionInfo,
            Database database) throws JMException {
        String path = connectionInfo.getName();
        if (!MBEANS.containsKey(path)) {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            String name = database.getShortName();
            ObjectName mbeanObjectName = getObjectName(name, path);
            MBEANS.put(path, mbeanObjectName);
            DatabaseInfo info = new DatabaseInfo(database);
            Object mbean = new DocumentedMBean(info, DatabaseInfoMBean.class);
            mbeanServer.registerMBean(mbean, mbeanObjectName);
        }
    }

    /**
     * Unregisters the MBean for the database if one is registered.
     *
     * @param name database name
     * @throws JMException on failure
     */
    public static void unregisterMBean(String name) throws Exception {
        ObjectName mbeanObjectName = MBEANS.remove(name);
        if (mbeanObjectName != null) {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            mbeanServer.unregisterMBean(mbeanObjectName);
        }
    }

    @Override
    public boolean isExclusive() {
        return database.getExclusiveSession() != null;
    }

    @Override
    public boolean isReadOnly() {
        return database.isReadOnly();
    }

    @Override
    public String getMode() {
        return database.getMode().getName();
    }

    @Override
    public int getTraceLevel() {
        return database.getTraceSystem().getLevelFile();
    }

    @Override
    public void setTraceLevel(int level) {
        database.getTraceSystem().setLevelFile(level);
    }

    @Override
    public long getFileWriteCount() {
        if (database.isPersistent()) {
            return database.getStore().getMvStore().getFileStore().getWriteCount();
        }
        return 0;
    }

    @Override
    public long getFileReadCount() {
        if (database.isPersistent()) {
            return database.getStore().getMvStore().getFileStore().getReadCount();
        }
        return 0;
    }

    @Override
    public long getFileSize() {
        long size = 0;
        if (database.isPersistent()) {
            size = database.getStore().getMvStore().getFileStore().size();
        }
        return size / 1024;
    }

    @Override
    public int getCacheSizeMax() {
        if (database.isPersistent()) {
            return database.getStore().getMvStore().getCacheSize() * 1024;
        }
        return 0;
    }

    @Override
    public void setCacheSizeMax(int kb) {
        if (database.isPersistent()) {
            database.setCacheSize(kb);
        }
    }

    @Override
    public int getCacheSize() {
        if (database.isPersistent()) {
            return database.getStore().getMvStore().getCacheSizeUsed() * 1024;
        }
        return 0;
    }

    @Override
    public String getVersion() {
        return Constants.FULL_VERSION;
    }

    @Override
    public String listSettings() {
        StringBuilder builder = new StringBuilder();
        for (Entry<String, String> e : database.getSettings().getSortedSettings()) {
            builder.append(e.getKey()).append(" = ").append(e.getValue()).append('\n');
        }
        return builder.toString();
    }

    @Override
    public String listSessions() {
        StringBuilder buff = new StringBuilder();
        for (SessionLocal session : database.getSessions(false)) {
            buff.append("session id: ").append(session.getId());
            buff.append(" user: ").
                    append(session.getUser().getName()).
                    append('\n');
            NetworkConnectionInfo networkConnectionInfo = session.getNetworkConnectionInfo();
            if (networkConnectionInfo != null) {
                buff.append("server: ").append(networkConnectionInfo.getServer()).append('\n') //
                        .append("clientAddr: ").append(networkConnectionInfo.getClient()).append('\n');
                String clientInfo = networkConnectionInfo.getClientInfo();
                if (clientInfo != null) {
                    buff.append("clientInfo: ").append(clientInfo).append('\n');
                }
            }
            buff.append("connected: ").
                    append(session.getSessionStart().getString()).
                    append('\n');
            Command command = session.getCurrentCommand();
            if (command != null) {
                buff.append("statement: ")
                        .append(command)
                        .append('\n')
                        .append("started: ")
                        .append(session.getCommandStartOrEnd().getString())
                        .append('\n');
            }
            for (Table table : session.getLocks()) {
                if (table.isLockedExclusivelyBy(session)) {
                    buff.append("write lock on ");
                } else {
                    buff.append("read lock on ");
                }
                buff.append(table.getSchema().getName()).
                        append('.').append(table.getName()).
                        append('\n');
            }
            buff.append('\n');
        }
        return buff.toString();
    }

}
