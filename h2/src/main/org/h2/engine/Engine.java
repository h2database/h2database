/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.HashMap;

import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.util.StringUtils;

/**
 * The engine contains a map of all open databases.
 * It is also responsible for opening and creating new databases.
 * This is a singleton class.
 */
public class Engine {
    // TODO use a 'engine'/'master' database to allow shut down the server, view & kill sessions and so on

    private static final Engine INSTANCE = new Engine();
    private final HashMap databases = new HashMap();

    private Engine() {
        // don't allow others to instantiate
    }

    public static Engine getInstance() {
        return INSTANCE;
    }

    private Session openSession(ConnectionInfo ci, boolean ifExists, String cipher) throws SQLException {
        // may not remove properties here, otherwise they are lost if it is required to call it twice
        String name = ci.getName();
        Database database;
        if (ci.isUnnamed()) {
            database = null;
        } else {
            database = (Database) databases.get(name);
        }
        User user = null;
        boolean opened = false;
        if (database == null) {
            if (ifExists && !Database.exists(name)) {
                throw Message.getSQLException(ErrorCode.DATABASE_NOT_FOUND_1, name);
            }
            database = new Database(name, ci, cipher);
            opened = true;
            if (database.getAllUsers().size() == 0) {
                // users is the last thing we add, so if no user is around, the database is not initialized correctly
                user = new User(database, database.allocateObjectId(false, true), ci.getUserName(), false);
                user.setAdmin(true);
                user.setUserPasswordHash(ci.getUserPasswordHash());
                database.setMasterUser(user);
            }
            if (!ci.isUnnamed()) {
                databases.put(name, database);
            }
            database.opened();
        }
        synchronized (database) {
            if (database.isClosing()) {
                return null;
            }
            if (user == null) {
                try {
                    database.checkFilePasswordHash(cipher, ci.getFilePasswordHash());
                    // create the exception here so it is not possible from the stack trace
                    // to see if the user name was wrong or the password
                    SQLException wrongUserOrPassword = Message.getSQLException(ErrorCode.WRONG_USER_OR_PASSWORD);
                    user = database.getUser(ci.getUserName(), wrongUserOrPassword);
                    user.checkUserPasswordHash(ci.getUserPasswordHash(), wrongUserOrPassword);
                    if (opened && !user.getAdmin()) {
                        // reset - because the user is not an admin, and has no
                        // right to listen to exceptions
                        database.setEventListener(null);
                    }
                } catch (SQLException e) {
                    database.removeSession(null);
                    throw e;
                }
            }
            checkClustering(ci, database);
            Session session = database.createSession(user);
            return session;
        }
    }

    public synchronized Session getSession(ConnectionInfo ci) throws SQLException {
        boolean ifExists = ci.removeProperty("IFEXISTS", false);
        boolean ignoreUnknownSetting = ci.removeProperty("IGNORE_UNKNOWN_SETTINGS", false);
        String cipher = ci.removeProperty("CIPHER", null);
        Session session;
        while (true) {
            session = openSession(ci, ifExists, cipher);
            if (session != null) {
                break;
            }
            // we found a database that is currently closing
            // wait a bit to avoid a busy loop (the method is synchronized)
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        String[] keys = ci.getKeys();
        session.setAllowLiterals(true);
        for (int i = 0; i < keys.length; i++) {
            String setting = keys[i];
            String value = ci.getProperty(setting);
            try {
                CommandInterface command = session.prepareCommand("SET " + Parser.quoteIdentifier(setting) + " "
                        + value, Integer.MAX_VALUE);
                command.executeUpdate();
            } catch (SQLException e) {
                if (!ignoreUnknownSetting) {
                    session.close();
                    throw e;
                }
            }
        }
        session.setAllowLiterals(false);
        session.commit(true);
        session.getDatabase().getTrace(Trace.SESSION).info("connected #" + session.getId());
        return session;
    }

    private void checkClustering(ConnectionInfo ci, Database database) throws SQLException {
        String clusterSession = ci.getProperty(SetTypes.CLUSTER, null);
        if (Constants.CLUSTERING_DISABLED.equals(clusterSession)) {
            // in this case, no checking is made
            // (so that a connection can be made to disable/change clustering)
            return;
        }
        String clusterDb = database.getCluster();
        if (!Constants.CLUSTERING_DISABLED.equals(clusterDb)) {
            if (!StringUtils.equals(clusterSession, clusterDb)) {
                if (clusterDb.equals(Constants.CLUSTERING_DISABLED)) {
                    throw Message.getSQLException(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_ALONE);
                } else {
                    throw Message.getSQLException(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1, clusterDb);
                }
            }
        }
    }

    public void close(String name) {
        databases.remove(name);
    }

}
