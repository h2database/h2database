/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.dml.SetTypes;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.schema.Schema;
import org.h2.store.DataHandler;
import org.h2.store.InDoubtTransaction;
import org.h2.store.LogSystem;
import org.h2.store.UndoLog;
import org.h2.store.UndoLogRecord;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * @author Thomas
 */
public class Session implements SessionInterface {
    private User user;
    private int id;
    private Database database;
    private ObjectArray locks = new ObjectArray();
    // TODO big transactions: rollback log should be a sorted result set,
    // so that big transactions are possible (delete or update huge tables)
    private UndoLog undoLog;
    private boolean autoCommit = true;
    // TODO function RANDOM: use a own implementation, making it system independent
    private Random random;
    private LogSystem logSystem;
    private int lockTimeout;
    private long lastIdentity;
    private int firstUncommittedLog = LogSystem.LOG_WRITTEN;
    private int firstUncommittedPos = LogSystem.LOG_WRITTEN;
    private HashMap savepoints;
    private Exception stackTrace = new Exception();
    private HashMap localTempTables;
    private int throttle;
    private long lastThrottle;
    private Command currentCommand;
    private boolean allowLiterals;
    private String currentSchemaName;
    private String traceModuleName;
    private HashSet unlinkSet;

    public Table findLocalTempTable(String name) {
        Table t = null;
        if(t == null && localTempTables != null) {
            t = (Table) localTempTables.get(name);
        }
        return t;
    }

    public ObjectArray getLocalTempTables() {
        if(localTempTables == null) {
            return new ObjectArray();
        }
        ObjectArray list = new ObjectArray(localTempTables.values());
        return list;
    }

    public String getNewLocalTempTransactionTableName() {
        // TODO this is current not used
        String name = Constants.TEMP_TABLE_TRANSACTION_PREFIX;
        for(int i=0; ; i++) {
            String n = name + i;
            if(findLocalTempTable(n) == null) {
                return n;
            }
        }
    }

    public void addLocalTempTable(TableData table) throws SQLException {
        cleanTempTables();
        if(localTempTables == null) {
            localTempTables = new HashMap();
        }
        if(localTempTables.get(table.getName()) != null) {
            throw Message.getSQLException(Message.TABLE_OR_VIEW_ALREADY_EXISTS_1, table.getSQL());
        }
        localTempTables.put(table.getName(), table);
    }

    public void removeLocalTempTable(Table table) throws SQLException {
        localTempTables.remove(table.getName());
        table.removeChildrenAndResources(this);
    }

    public void finalize() {
        if(!Constants.RUN_FINALIZERS) {
            return;
        }
        if(database != null) {
            throw Message.getInternalError("not closed", stackTrace);
        }
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public User getUser() {
        return user;
    }

    public void setAutoCommit(boolean b) {
        autoCommit = b;
    }

    public int getLockTimeout() {
        return lockTimeout;
    }

    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    public Session() {

    }

    public SessionInterface createSession(ConnectionInfo ci) throws SQLException {
        return Engine.getInstance().getSession(ci);
    }

    Session(Database database, User user, int id) {
        this.database = database;
        this.undoLog = new UndoLog(this);
        this.user = user;
        this.id = id;
        this.logSystem = database.getLog();
        Setting setting = database.findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_LOCK_TIMEOUT));
        this.lockTimeout = setting == null ? Constants.INITIAL_LOCK_TIMEOUT : setting.getIntValue();
        this.currentSchemaName = Constants.SCHEMA_MAIN;
    }
    
    public CommandInterface prepareCommand(String sql) throws SQLException {
        return prepareLocal(sql);
    }

    public Prepared prepare(String sql) throws SQLException {
        return prepare(sql, false);
    }

    public Prepared prepare(String sql, boolean rightsChecked) throws SQLException {
        Parser parser = new Parser(this);
        parser.setRightsChecked(rightsChecked);
        return parser.prepare(sql);
    }

    public Command prepareLocal(String sql) throws SQLException {
        if(database == null) {
            throw Message.getSQLException(Message.CONNECTION_BROKEN);
        }
        Parser parser = new Parser(this);
        return parser.prepareCommand(sql);
    }

    public Database getDatabase() {
        return database;
    }

    public int getPowerOffCount() {
        return database == null ? 0 : database.getPowerOffCount();
    }

    public void setPowerOffCount(int count) {
        if(database != null) {
            database.setPowerOffCount(count);
        }
    }

    public void commit() throws SQLException {
        if(containsUncommitted()) {
            // need to commit even if rollback is not possible (create/drop table and so on)
            logSystem.commit(this);
        }
        if(undoLog.size() > 0) {
            undoLog.clear();
            cleanTempTables();
        }
        if(unlinkSet != null && unlinkSet.size() > 0) {
            // need to flush the log file, because we can't unlink lobs if the commit record is not written
            logSystem.flush();
            Iterator it = unlinkSet.iterator();
            while(it.hasNext()) {
                Value v = (Value) it.next();
                v.unlink(database);
            }
            unlinkSet = null;
        }
        unlockAll();
    }

    public void rollback() throws SQLException {
        boolean needCommit = false;
        if (undoLog.size() > 0) {
            rollbackTo(0);
            needCommit = true;
        }
        if(locks.size() > 0 || needCommit) {
            logSystem.commit(this);
        }
        cleanTempTables();
        unlockAll();
    }

    public void rollbackTo(int index) throws SQLException {
        while (undoLog.size() > index) {
            UndoLogRecord entry = undoLog.getAndRemoveLast();
            entry.undo(this);
        }
        if(savepoints != null) {
            String[] names = new String[savepoints.size()];
            savepoints.keySet().toArray(names);
            for(int i=0; i<names.length; i++) {
                String name = names[i];
                Integer id = (Integer) savepoints.get(names[i]);
                if(id.intValue() > index) {
                    savepoints.remove(name);
                }
            }
        }
    }

    public int getLogId() {
        return undoLog.size();
    }

    public int getId() {
        return id;
    }

    public void close() throws SQLException {
        if(database != null) {
            try {
                database.removeSession(this);
            } finally {
                database = null;
            }
        }
    }

    public void addLock(Table table) {
        if(Constants.CHECK) {
            if(locks.indexOf(table)>=0) {
                throw Message.getInternalError();
            }
        }
        locks.add(table);
    }

    public void log(UndoLogRecord log) throws SQLException {
        // called _after_ the row was inserted successfully into the table,
        // otherwise rollback will try to rollback a not-inserted row
        if(Constants.CHECK) {
            int lockMode = database.getLockMode();
            if(lockMode != Constants.LOCK_MODE_OFF) {
                if(locks.indexOf(log.getTable())<0 && log.getTable().getTableType() != Table.TABLE_LINK) {
                    throw Message.getInternalError();
                }
            }
        }
        undoLog.add(log);
    }

    private void unlockAll() throws SQLException {
        if(Constants.CHECK) {
            if(undoLog.size() > 0) {
                throw Message.getInternalError();
            }
        }
        for(int i=0; i<locks.size(); i++) {
            Table t = (Table)locks.get(i);
            t.unlock(this);
        }
        locks.clear();
        savepoints = null;
    }

    private void cleanTempTables() throws SQLException {
        if(localTempTables != null && localTempTables.size()>0) {
            ObjectArray list = new ObjectArray(localTempTables.values());
            for(int i=0; i<list.size(); i++) {
                TableData table = (TableData) list.get(i);
                if(table.isOnCommitDrop()) {
                    table.setModified();
                    localTempTables.remove(table.getName());
                    table.removeChildrenAndResources(this);
                } else if(table.isOnCommitTruncate()) {
                    table.truncate(this);
                }
            }
        }
    }

    public Random getRandom() {
        if(random == null) {
            random = new Random();
        }
        return random;
    }

    public Trace getTrace() {
        if(traceModuleName == null) {
            traceModuleName = Trace.JDBC + "[" + id + "]";
        }
        if(database == null) {
            return new TraceSystem(null).getTrace(traceModuleName);
        }
        return database.getTrace(traceModuleName);
    }

    public void setLastIdentity(long last) {
        this.lastIdentity = last;
    }

    public long getLastIdentity() {
        return lastIdentity;
    }

    public void addLogPos(int logId, int pos) {
        if (firstUncommittedLog == LogSystem.LOG_WRITTEN) {
            firstUncommittedLog = logId;
            firstUncommittedPos = pos;
        }
    }

    public int getFirstUncommittedLog() {
        return firstUncommittedLog;
    }

    public int getFirstUncommittedPos() {
        return firstUncommittedPos;
    }

    public void setAllCommitted() {
        firstUncommittedLog = LogSystem.LOG_WRITTEN;
        firstUncommittedPos = LogSystem.LOG_WRITTEN;
    }

    private boolean containsUncommitted() {
        return firstUncommittedLog != LogSystem.LOG_WRITTEN;
    }

    public void addSavepoint(String name) {
        if(savepoints == null) {
            savepoints = new HashMap();
        }
        savepoints.put(name, new Integer(getLogId()));
    }

    public void rollbackToSavepoint(String name) throws SQLException {
        if(savepoints == null) {
            throw Message.getSQLException(Message.SAVEPOINT_IS_INVALID_1, name);
        }
        Integer id = (Integer)savepoints.get(name);
        if(id==null) {
            throw Message.getSQLException(Message.SAVEPOINT_IS_INVALID_1, name);
        }
        int i = id.intValue();
        rollbackTo(i);
    }

    public void prepareCommit(String transactionName) throws SQLException {
        if(containsUncommitted()) {
            // need to commit even if rollback is not possible (create/drop table and so on)
            logSystem.prepareCommit(this, transactionName);
        }
    }

    public void setPreparedTransaction(String transactionName, boolean commit) throws SQLException {
        ObjectArray list = logSystem.getInDoubtTransactions();
        int state = commit ? InDoubtTransaction.COMMIT : InDoubtTransaction.ROLLBACK;
        for(int i=0; list!=null && i<list.size(); i++) {
            InDoubtTransaction p = (InDoubtTransaction)list.get(i);
            if(p.getTransaction().equals(transactionName)) {
                p.setState(state);
            }
        }
    }

    public boolean isClosed() {
        return database == null;
    }

    public void setThrottle(int throttle) {
        this.throttle = throttle;
    }

    public void throttle() {
        if(throttle == 0) {
            return;
        }
        long time = System.currentTimeMillis();
        if(lastThrottle + Constants.THROTTLE_DELAY > time) {
            return;
        }
        lastThrottle = time + throttle;
        try {
            Thread.sleep(throttle);
        } catch(Exception e) {
            // ignore
        }
    }

    public void setCurrentCommand(Command command) {
        this.currentCommand = command;
    }

    public void checkCancelled() throws SQLException {
        if(currentCommand != null) {
            currentCommand.checkCancelled();
        }
    }

    public boolean getAllowLiterals() {
        return allowLiterals;
    }

    public void setAllowLiterals(boolean b) {
        this.allowLiterals = b;
    }

    public void setCurrentSchema(Schema schema) {
        this.currentSchemaName = schema.getName();
    }

    public String getCurrentSchemaName() {
        return currentSchemaName;
    }
    
    public JdbcConnection createConnection(boolean columnList) throws SQLException {
        String url;
        if(columnList) {
            url = Constants.CONN_URL_INTERNAL;
        } else {
            url = Constants.CONN_URL_INTERNAL;
        }
        return new JdbcConnection(this, getUser().getName(), url);
    }
    
    public DataHandler getDataHandler() {
        return database;
    }

    public void unlinkAtCommit(Value v) {
        if(unlinkSet == null) {
            unlinkSet = new HashSet();
        }
        unlinkSet.add(v);
    }
    
    public void unlinkAtCommitStop(Value v) {
        if(unlinkSet != null) {
            unlinkSet.remove(v);
        }
    }

}
