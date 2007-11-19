/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

//#ifdef JDK14
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
import org.h2.util.ByteUtils;
import org.h2.util.JdbcUtils;
import org.h2.jdbc.JdbcConnection;
//#endif

import org.h2.message.TraceObject;

//#ifdef JDK16
/*
import javax.sql.StatementEventListener;
*/
//#endif

/**
 * This class provides support for distributed transactions. 
 * An application developer usually does not use this interface.
 * It is used by the transaction manager internally.
 */
public class JdbcXAConnection extends TraceObject 
//#ifdef JDK14
implements XAConnection, XAResource, JdbcConnectionListener
//#endif
{
    
//#ifdef JDK14
    private JdbcDataSourceFactory factory;
    private String url, user, password;
    private JdbcConnection connSentinel;
    private JdbcConnection conn;
    private ArrayList listeners = new ArrayList();
    private Xid currentTransaction;
    private int currentTransactionId;
    private static int nextTransactionId;
    
    static {
        org.h2.Driver.load();
    }
    
    JdbcXAConnection(JdbcDataSourceFactory factory, int id, String url, String user, String password) throws SQLException {
        this.factory = factory;
        setTrace(factory.getTrace(), TraceObject.XA_DATA_SOURCE, id);
        this.url = url;
        this.user = user;
        this.password = password;
        connSentinel = openConnection();
        getConnection();        
    }
//#endif

    /**
     * Get the XAResource object.
     * 
     * @return itself
     */
//#ifdef JDK14
    public XAResource getXAResource() throws SQLException {
        debugCodeCall("getXAResource");
        return this;
    }
//#endif

    /**
     * Close the physical connection.
     * This method is usually called by the connection pool.
     */
//#ifdef JDK14
    public void close() throws SQLException {
        debugCodeCall("close");
        try {
            closeConnection(conn);
            closeConnection(connSentinel);
        } finally {
            conn = null;
            connSentinel = null;
        }
    }
//#endif
    
    /**
     * Get a new connection.
     * This method is usually called by the connection pool when there are no more connections in the pool.
     * 
     * @return the connection
     */
//#ifdef JDK14
    public Connection getConnection() throws SQLException {
        debugCodeCall("getConnection");
        if (conn != null) {
            closeConnection(conn);
            conn = null;
        }
        conn = openConnection();
        conn.setJdbcConnectionListener(this);
        return conn;
    }
//#endif
    
    /**
     * Register a new listener for the connection.
     * 
     * @param listener the event listener
     */
//#ifdef JDK14
    public void addConnectionEventListener(ConnectionEventListener listener) {
        debugCode("addConnectionEventListener(listener)");
        listeners.add(listener);
        if (conn != null) {
            conn.setJdbcConnectionListener(this);
        }
    }
//#endif
    
    /**
     * Remove the event listener.
     * 
     * @param listener the event listener
     */
//#ifdef JDK14
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        debugCode("removeConnectionEventListener(listener)");
        listeners.remove(listener);
    }
//#endif
    
    /**
     * INTERNAL
     */
//#ifdef JDK14
    public void fatalErrorOccurred(JdbcConnection conn, SQLException e) throws SQLException {
        debugCode("fatalErrorOccurred(conn, e)");
        for (int i = 0; i < listeners.size(); i++) {
            ConnectionEventListener listener = (ConnectionEventListener) listeners.get(i);
            ConnectionEvent event = new ConnectionEvent(this, e);
            listener.connectionErrorOccurred(event);
        }
        close();
    }
//#endif
    
    /**
     * INTERNAL
     */
//#ifdef JDK14
    public void closed(JdbcConnection conn) {
        debugCode("closed(conn)");
        for (int i = 0; i < listeners.size(); i++) {
            ConnectionEventListener listener = (ConnectionEventListener) listeners.get(i);
            ConnectionEvent event = new ConnectionEvent(this);
            listener.connectionClosed(event);
        }
    }
//#endif
    
    /**
     * Get the transaction timeout.
     * 
     * @return 0
     */
//#ifdef JDK14
    public int getTransactionTimeout() throws XAException {
        debugCodeCall("getTransactionTimeout");
        return 0;
    }
//#endif

    /**
     * Set the transaction timeout.
     * 
     * @param seconds ignored
     * @return false
     */
//#ifdef JDK14
    public boolean setTransactionTimeout(int seconds) throws XAException {
        debugCodeCall("setTransactionTimeout", seconds);
        return false;
    }
//#endif
    
    /**
     * Checks if this is the same XAResource.
     * 
     * @param xares the other object
     * @return true if this is the same object
     */
//#ifdef JDK14
    public boolean isSameRM(XAResource xares) throws XAException {
        debugCode("isSameRM(xares)");
        return xares == this;
    }
//#endif
    
    /**
     * Get the list of prepared transaction branches.
     * This method is called by the transaction manager during recovery.
     * 
     * @param flag TMSTARTRSCAN, TMENDRSCAN, or TMNOFLAGS. If no other flags are set,
     *  TMNOFLAGS must be used.
     *  @return zero or more Xid objects
     */
//#ifdef JDK14
    public Xid[] recover(int flag) throws XAException {
        debugCodeCall("recover", quoteFlags(flag));
        checkOpen();
        Statement stat = null;
        try {
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT ORDER BY TRANSACTION");
            ArrayList list = new ArrayList();
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
            getTrace().debug("throw XAException.XAER_RMERR", e);
           throw new XAException(XAException.XAER_RMERR);
        } finally {
            JdbcUtils.closeSilently(stat);
        }
    }
//#endif
    
    /**
     * Prepare a transaction.
     * 
     * @param xid the transaction id
     */
//#ifdef JDK14
    public int prepare(Xid xid) throws XAException {
        debugCode("prepare("+quoteXid(xid)+")");
        checkOpen();
        if (!currentTransaction.equals(xid)) {
            getTrace().debug("throw XAException.XAER_INVAL");
            throw new XAException(XAException.XAER_INVAL);
        }
        Statement stat = null;
        try {
            stat = conn.createStatement();
            currentTransactionId = nextTransactionId++;
            stat.execute("PREPARE COMMIT TX_" + currentTransactionId);
        } catch (SQLException e) {
            throw convertException(e);
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        getTrace().debug("return XA_OK");     
        return XA_OK;
    }
//#endif
    
    /**
     * Forget a transaction.
     * This method does not have an effect for this database.
     * 
     * @param xid the transaction id
     */
//#ifdef JDK14
    public void forget(Xid xid) throws XAException {
        debugCode("forget("+quoteXid(xid)+")");
    }
//#endif
    
    /**
     * Roll back a transaction.
     * 
     * @param xid the transaction id
     */
//#ifdef JDK14
    public void rollback(Xid xid) throws XAException {
        debugCode("rollback("+quoteXid(xid)+")");
        try {
            conn.rollback();
        } catch (SQLException e) {
            throw convertException(e);
        }
        getTrace().debug("rolled back");
        currentTransaction = null;        
    }
//#endif
    
    /**
     * End a transaction.
     * 
     * @param xid the transaction id
     * @param flags TMSUCCESS, TMFAIL, or TMSUSPEND
     */
//#ifdef JDK14
    public void end(Xid xid, int flags) throws XAException {
        debugCode("end("+quoteXid(xid)+", "+quoteFlags(flags)+")");
        // TODO transaction end: implement this method
        if (flags == TMSUSPEND) {
            return;
        }
        if (!currentTransaction.equals(xid)) {
            getTrace().debug("throw XAException.XAER_OUTSIDE");
            throw new XAException(XAException.XAER_OUTSIDE);
        }
    }
//#endif
    
    /**
     * Start or continue to work on a transaction.
     * 
     * @param xid the transaction id
     * @param flags TMNOFLAGS, TMJOIN, or TMRESUME
     */
//#ifdef JDK14
    public void start(Xid xid, int flags) throws XAException {
        debugCode("start("+quoteXid(xid)+", "+quoteFlags(flags)+")");
        if (flags == TMRESUME) {
            return;
        }
        if (currentTransaction != null) {
            getTrace().debug("throw XAException.XAER_NOTA");
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw convertException(e);
        }
        getTrace().debug("currentTransaction=xid");
        currentTransaction = xid;
    }
//#endif
    
    /**
     * Commit a transaction.
     * 
     * @param xid the transaction id
     * @param onePhase use a one-phase protocol if true
     */
//#ifdef JDK14
    public void commit(Xid xid, boolean onePhase) throws XAException {
        debugCode("commit("+quoteXid(xid)+", "+onePhase+")");
        Statement stat = null;
        try {
            if (onePhase) {
                conn.commit();
            } else {
                stat = conn.createStatement();
                stat.execute("COMMIT TRANSACTION TX_" + currentTransactionId);
            }
        } catch (SQLException e) {
            throw convertException(e);
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        getTrace().debug("committed");
        currentTransaction = null;        
    }
//#endif

//#ifdef JDK16
/*
    public void addStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }
*/
//#endif

//#ifdef JDK16
/*
    public void removeStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }
*/
//#endif
    
    /**
     * INTERNAL
     */
//#ifdef JDK14
    public String toString() {
        return getTraceObjectName() + ": url=" + url + " user=" + user;
    }
    
    private void closeConnection(JdbcConnection conn) throws SQLException {
        if (conn != null) {
            conn.closeConnection();
        }
    }
    
    private JdbcConnection openConnection() throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        JdbcConnection conn = new JdbcConnection(url, info);
        conn.setJdbcConnectionListener(this);
        if (currentTransaction != null) {
            conn.setAutoCommit(false);
        }
        return conn;
    }

    private XAException convertException(SQLException e) {
        getTrace().debug("throw XAException("+e.getMessage()+")");
        return new XAException(e.getMessage());
    }

    private String quoteXid(Xid xid) {
        StringBuffer buff = new StringBuffer();
        buff.append("\"f:");
        buff.append(xid.getFormatId());
        buff.append(",bq:");
        buff.append(ByteUtils.convertBytesToString(xid.getBranchQualifier()));
        buff.append(",gx:");
        buff.append(ByteUtils.convertBytesToString(xid.getGlobalTransactionId()));
        buff.append(",c:");
        buff.append(xid.getClass().getName());
        buff.append("\"");
        return buff.toString();
    }
    
    private String quoteFlags(int flags) {
        StringBuffer buff = new StringBuffer();
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
        if ((flags & XAResource.XA_OK) != 0) {
            buff.append("|XAResource.XA_OK");
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
        if (conn == null) {
            getTrace().debug("conn==null");
            throw new XAException(XAException.XAER_RMERR);
        }
    }
//#endif

}
