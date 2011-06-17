/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

//## Java 1.4 begin ##
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.jdbc.JdbcConnection;
import org.h2.util.ByteUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.StringUtils;
//## Java 1.4 end ##

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;

/*## Java 1.6 begin ##
import javax.sql.StatementEventListener;
## Java 1.6 end ##*/

/**
 * This class provides support for distributed transactions.
 * An application developer usually does not use this interface.
 * It is used by the transaction manager internally.
 */
public class JdbcXAConnection extends TraceObject
//## Java 1.4 begin ##
implements XAConnection, XAResource
//## Java 1.4 end ##
{

//## Java 1.4 begin ##
    private static int nextTransactionId;

    private JdbcDataSourceFactory factory;
    private String url, user;
    private JdbcConnection physicalConn;
    private PooledJdbcConnection handleConn;
    private ArrayList<ConnectionEventListener> listeners = New.arrayList();
    private Xid currentTransaction;
    private int currentTransactionId;

    static {
        org.h2.Driver.load();
    }

    JdbcXAConnection(JdbcDataSourceFactory factory, int id, String url, String user, char[] password) throws SQLException {
        this.factory = factory;
        setTrace(factory.getTrace(), TraceObject.XA_DATA_SOURCE, id);
        this.url = url;
        this.user = user;
        Properties info = new Properties();
        info.setProperty("user", user);
        info.put("password", StringUtils.cloneCharArray(password));
        physicalConn = new JdbcConnection(url, info);
    }
//## Java 1.4 end ##

    /**
     * Get the XAResource object.
     *
     * @return itself
     */
//## Java 1.4 begin ##
    public XAResource getXAResource() {
        debugCodeCall("getXAResource");
        return this;
    }
//## Java 1.4 end ##

    /**
     * Close the physical connection.
     * This method is usually called by the connection pool.
     *
     * @throws SQLException
     */
//## Java 1.4 begin ##
    public void close() throws SQLException {
        debugCodeCall("close");
        if (handleConn != null) {
            listeners.clear();
            handleConn.close();
        }
        if (physicalConn != null) {
            try {
                physicalConn.close();
            } finally {
                physicalConn = null;
            }
        }
    }
//## Java 1.4 end ##

    /**
     * Get a connection that is a handle to the physical connection. This method
     * is usually called by the connection pool. This method closes the last
     * connection handle if one exists.
     *
     * @return the connection
     */
//## Java 1.4 begin ##
    public Connection getConnection() throws SQLException {
        debugCodeCall("getConnection");
        if (handleConn != null) {
            handleConn.close();
        }
        handleConn = new PooledJdbcConnection(physicalConn);
        return handleConn;
    }
//## Java 1.4 end ##

    /**
     * Register a new listener for the connection.
     *
     * @param listener the event listener
     */
//## Java 1.4 begin ##
    public void addConnectionEventListener(ConnectionEventListener listener) {
        debugCode("addConnectionEventListener(listener);");
        listeners.add(listener);
    }
//## Java 1.4 end ##

    /**
     * Remove the event listener.
     *
     * @param listener the event listener
     */
//## Java 1.4 begin ##
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        debugCode("removeConnectionEventListener(listener);");
        listeners.remove(listener);
    }
//## Java 1.4 end ##

    /**
     * INTERNAL
     */
//## Java 1.4 begin ##
    void closedHandle() {
        debugCode("closedHandle();");
        for (ConnectionEventListener listener : New.arrayList(listeners)) {
            ConnectionEvent event = new ConnectionEvent(this);
            listener.connectionClosed(event);
        }
        handleConn = null;
    }
//## Java 1.4 end ##

    /**
     * Get the transaction timeout.
     *
     * @return 0
     */
//## Java 1.4 begin ##
    public int getTransactionTimeout() {
        debugCodeCall("getTransactionTimeout");
        return 0;
    }
//## Java 1.4 end ##

    /**
     * Set the transaction timeout.
     *
     * @param seconds ignored
     * @return false
     */
//## Java 1.4 begin ##
    public boolean setTransactionTimeout(int seconds) {
        debugCodeCall("setTransactionTimeout", seconds);
        return false;
    }
//## Java 1.4 end ##

    /**
     * Checks if this is the same XAResource.
     *
     * @param xares the other object
     * @return true if this is the same object
     */
//## Java 1.4 begin ##
    public boolean isSameRM(XAResource xares) {
        debugCode("isSameRM(xares);");
        return xares == this;
    }
//## Java 1.4 end ##

    /**
     * Get the list of prepared transaction branches.
     * This method is called by the transaction manager during recovery.
     *
     * @param flag TMSTARTRSCAN, TMENDRSCAN, or TMNOFLAGS. If no other flags are set,
     *  TMNOFLAGS must be used.
     *  @return zero or more Xid objects
     * @throws XAException
     */
//## Java 1.4 begin ##
    public Xid[] recover(int flag) throws XAException {
        debugCodeCall("recover", quoteFlags(flag));
        checkOpen();
        Statement stat = null;
        try {
            stat = physicalConn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT ORDER BY TRANSACTION");
            ArrayList<Xid> list = New.arrayList();
            while (rs.next()) {
                String tid = rs.getString("TRANSACTION");
                int id = getNextId(XID);
                Xid xid = new JdbcXid(factory, id, tid);
                list.add(xid);
            }
            rs.close();
            Xid[] result = new Xid[list.size()];
            list.toArray(result);
            return result;
        } catch (SQLException e) {
            throw new XAException(XAException.XAER_RMERR);
        } finally {
            JdbcUtils.closeSilently(stat);
        }
    }
//## Java 1.4 end ##

    /**
     * Prepare a transaction.
     *
     * @param xid the transaction id
     * @return XA_OK
     * @throws XAException
     */
//## Java 1.4 begin ##
    public int prepare(Xid xid) throws XAException {
        if (isDebugEnabled()) {
            debugCode("prepare("+quoteXid(xid)+");");
        }
        checkOpen();
        if (!currentTransaction.equals(xid)) {
            throw new XAException(XAException.XAER_INVAL);
        }
        Statement stat = null;
        try {
            stat = physicalConn.createStatement();
            currentTransactionId = nextTransactionId++;
            stat.execute("PREPARE COMMIT TX_" + currentTransactionId);
        } catch (SQLException e) {
            throw convertException(e);
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        return XA_OK;
    }
//## Java 1.4 end ##

    /**
     * Forget a transaction.
     * This method does not have an effect for this database.
     *
     * @param xid the transaction id
     */
//## Java 1.4 begin ##
    public void forget(Xid xid) {
        if (isDebugEnabled()) {
            debugCode("forget("+quoteXid(xid)+");");
        }
    }
//## Java 1.4 end ##

    /**
     * Roll back a transaction.
     *
     * @param xid the transaction id
     * @throws XAException
     */
//## Java 1.4 begin ##
    public void rollback(Xid xid) throws XAException {
        if (isDebugEnabled()) {
            debugCode("rollback("+quoteXid(xid)+");");
        }
        try {
            physicalConn.rollback();
        } catch (SQLException e) {
            throw convertException(e);
        }
        currentTransaction = null;
    }
//## Java 1.4 end ##

    /**
     * End a transaction.
     *
     * @param xid the transaction id
     * @param flags TMSUCCESS, TMFAIL, or TMSUSPEND
     * @throws XAException
     */
//## Java 1.4 begin ##
    public void end(Xid xid, int flags) throws XAException {
        if (isDebugEnabled()) {
            debugCode("end("+quoteXid(xid)+", "+quoteFlags(flags)+");");
        }
        // TODO transaction end: implement this method
        if (flags == TMSUSPEND) {
            return;
        }
        if (!currentTransaction.equals(xid)) {
            throw new XAException(XAException.XAER_OUTSIDE);
        }
    }
//## Java 1.4 end ##

    /**
     * Start or continue to work on a transaction.
     *
     * @param xid the transaction id
     * @param flags TMNOFLAGS, TMJOIN, or TMRESUME
     * @throws XAException
     */
//## Java 1.4 begin ##
    public void start(Xid xid, int flags) throws XAException {
        if (isDebugEnabled()) {
            debugCode("start("+quoteXid(xid)+", "+quoteFlags(flags)+");");
        }
        if (flags == TMRESUME) {
            return;
        }
        if (currentTransaction != null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            physicalConn.setAutoCommit(false);
        } catch (SQLException e) {
            throw convertException(e);
        }
        currentTransaction = xid;
    }
//## Java 1.4 end ##

    /**
     * Commit a transaction.
     *
     * @param xid the transaction id
     * @param onePhase use a one-phase protocol if true
     * @throws XAException
     */
//## Java 1.4 begin ##
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (isDebugEnabled()) {
            debugCode("commit("+quoteXid(xid)+", "+onePhase+");");
        }
        Statement stat = null;
        try {
            if (onePhase) {
                physicalConn.commit();
            } else {
                stat = physicalConn.createStatement();
                stat.execute("COMMIT TRANSACTION TX_" + currentTransactionId);
            }
        } catch (SQLException e) {
            throw convertException(e);
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        currentTransaction = null;
    }
//## Java 1.4 end ##

    /**
     * [Not supported] Add a statement event listener.
     *
     * @param listener the new statement event listener
     */
/*## Java 1.6 begin ##
    public void addStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Remove a statement event listener.
     *
     * @param listener the statement event listener
     */
/*## Java 1.6 begin ##
    public void removeStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }
## Java 1.6 end ##*/

    /**
     * INTERNAL
     */
//## Java 1.4 begin ##
    public String toString() {
        return getTraceObjectName() + ": url=" + url + " user=" + user;
    }

    private XAException convertException(SQLException e) {
        XAException xa = new XAException(e.getMessage());
        xa.initCause(e);
        return xa;
    }

    private String quoteXid(Xid xid) {
        StringBuilder buff = new StringBuilder();
        buff.append("\"f:").
            append(xid.getFormatId()).
            append(",bq:").
            append(ByteUtils.convertBytesToString(xid.getBranchQualifier())).
            append(",gx:").
            append(ByteUtils.convertBytesToString(xid.getGlobalTransactionId())).
            append(",c:").
            append(xid.getClass().getName()).
            append("\"");
        return buff.toString();
    }

    private String quoteFlags(int flags) {
        StringBuilder buff = new StringBuilder();
        if ((flags & XAResource.TMENDRSCAN) != 0) {
            buff.append("|XAResource.TMENDRSCAN");
        }
        if ((flags & XAResource.TMFAIL) != 0) {
            buff.append("|XAResource.TMFAIL");
        }
        if ((flags & XAResource.TMJOIN) != 0) {
            buff.append("|XAResource.TMJOIN");
        }
        if ((flags & XAResource.TMONEPHASE) != 0) {
            buff.append("|XAResource.TMONEPHASE");
        }
        if ((flags & XAResource.TMRESUME) != 0) {
            buff.append("|XAResource.TMRESUME");
        }
        if ((flags & XAResource.TMSTARTRSCAN) != 0) {
            buff.append("|XAResource.TMSTARTRSCAN");
        }
        if ((flags & XAResource.TMSUCCESS) != 0) {
            buff.append("|XAResource.TMSUCCESS");
        }
        if ((flags & XAResource.TMSUSPEND) != 0) {
            buff.append("|XAResource.TMSUSPEND");
        }
        if ((flags & XAResource.XA_RDONLY) != 0) {
            buff.append("|XAResource.XA_RDONLY");
        }
        if (buff.length() == 0) {
            buff.append("|XAResource.TMNOFLAGS");
        }
        return buff.toString().substring(1);
    }

    private void checkOpen() throws XAException {
        if (physicalConn == null) {
            throw new XAException(XAException.XAER_RMERR);
        }
    }
//## Java 1.4 end ##

    /**
     * A pooled connection.
     */
    class PooledJdbcConnection extends JdbcConnection {

        private boolean isClosed;

        public PooledJdbcConnection(JdbcConnection conn) {
            super(conn);
            openStackTrace = new Exception("Stack Trace");
        }

        public synchronized void close() throws SQLException {
            if (!isClosed) {
                rollback();
                setAutoCommit(true);
                closedHandle();
                isClosed = true;
            }
        }

        public synchronized boolean isClosed() throws SQLException {
            return isClosed || super.isClosed();
        }

        protected synchronized void checkClosed(boolean write) throws SQLException {
            if (isClosed) {
                throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
            }
            super.checkClosed(write);
        }

        protected void finalize() {
            if (!SysProperties.runFinalize) {
                return;
            }
            Trace trace = getTrace();
            try {
                if (!isClosed()) {
                    trace.error("Pooled connection not closed", openStackTrace);
                    JdbcXAConnection.this.close();
                }
            } catch (SQLException e) {
                trace.debug("finalize", e);
            }
        }

    }

}
