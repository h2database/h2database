/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

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

import org.h2.jdbc.JdbcConnection;
import org.h2.message.TraceObject;
import org.h2.util.ByteUtils;

//#ifdef JDK16
/*
import javax.sql.StatementEventListener;
*/
//#endif

public class JdbcXAConnection extends TraceObject implements XAConnection, JdbcConnectionListener, XAResource {
    
    private JdbcDataSourceFactory factory;
    private String url, user, password;
    private JdbcConnection conn;
    private ArrayList listeners = new ArrayList();
    private Xid currentTransaction;
    
    JdbcXAConnection(JdbcDataSourceFactory factory, int id, String url, String user, String password) {
        this.factory = factory;
        setTrace(factory.getTrace(), TraceObject.XA_DATASOURCE, id);
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public XAResource getXAResource() throws SQLException {
        debugCodeCall("getXAResource");
        return this;
    }

    public void close() throws SQLException {
        debugCodeCall("close");
        if(conn != null) {
            conn.closeConnection();
            conn = null;
        }
    }

    public Connection getConnection() throws SQLException {
        debugCodeCall("getConnection");
        close();
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        conn = new JdbcConnection(url, info);        
        return conn;
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
        debugCode("addConnectionEventListener(listener)");
        listeners.add(listener);
        conn.setJdbcConnectionListener(this);
    }

    public void removeConnectionEventListener(ConnectionEventListener listener) {
        debugCode("removeConnectionEventListener(listener)");
        listeners.remove(listener);
    }

    public void fatalErrorOccured(JdbcConnection conn, SQLException e) throws SQLException {
        debugCode("fatalErrorOccured(conn, e)");
        for(int i=0; i<listeners.size(); i++) {
            ConnectionEventListener listener = (ConnectionEventListener)listeners.get(i);
            ConnectionEvent event = new ConnectionEvent(this, e);
            listener.connectionErrorOccurred(event);
        }
        close();
    }

    public void closed(JdbcConnection conn) {
        debugCode("closed(conn)");
        for(int i=0; i<listeners.size(); i++) {
            ConnectionEventListener listener = (ConnectionEventListener)listeners.get(i);
            ConnectionEvent event = new ConnectionEvent(this);
            listener.connectionClosed(event);
        }
    }

    public int getTransactionTimeout() throws XAException {
        debugCodeCall("getTransactionTimeout");
        return 0;
    }

    public boolean setTransactionTimeout(int seconds) throws XAException {
        debugCodeCall("setTransactionTimeout", seconds);
        return false;
    }

    public boolean isSameRM(XAResource xares) throws XAException {
        debugCode("isSameRM(xares)");
        return xares == this;
    }

    public Xid[] recover(int flag) throws XAException {
        debugCodeCall("recover", quoteFlags(flag));
        checkOpen();
        try {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT ORDER BY ID");
            ArrayList list = new ArrayList();
            while(rs.next()) {
                String tid = rs.getString("TRANSACTION");
                int id = getNextId(XID);
                Xid xid = new JdbcXid(factory, id, tid);
                list.add(xid);
            }
            Xid[] result = new Xid[list.size()];
            list.toArray(result);
            return result;
        } catch(SQLException e) {
            getTrace().debug("throw XAException.XAER_OUTSIDE", e);
           throw new XAException(XAException.XAER_OUTSIDE);
        }
    }
    
    private void checkOpen() throws XAException {
        if(conn == null) {
            getTrace().debug("conn==null");
            throw new XAException(XAException.XAER_OUTSIDE);
        }
    }

    public int prepare(Xid xid) throws XAException {
        debugCode("prepare("+quoteXid(xid)+")");
        checkOpen();
        if(currentTransaction != xid) {
            getTrace().debug("throw XAException.XAER_INVAL");
            throw new XAException(XAException.XAER_INVAL);
        }
        try {
            conn.createStatement().execute("PREPARE COMMIT");
        } catch(SQLException e) {
            throw convertException(e);
        }
        getTrace().debug("return TMSUCCESS");        
        return TMSUCCESS;
    }

    public void forget(Xid xid) throws XAException {
        debugCode("forget("+quoteXid(xid)+")");
        // TODO
    }

    public void rollback(Xid xid) throws XAException {
        debugCode("rollback("+quoteXid(xid)+")");
        try {
            conn.rollback();
        } catch(SQLException e) {
            throw convertException(e);
        }
        getTrace().debug("rolled back");
    }

    public void end(Xid xid, int flags) throws XAException {
        debugCode("end("+quoteXid(xid)+", "+quoteFlags(flags)+")");
        if(flags == TMSUSPEND) {
            return;
        }
        if(currentTransaction != xid) {
            getTrace().debug("throw XAException.XAER_OUTSIDE");
            throw new XAException(XAException.XAER_OUTSIDE);
        }
        getTrace().debug("currentTransaction=null");
        currentTransaction = null;
    }
    
    private String quoteFlags(int flags) {
        StringBuffer buff = new StringBuffer();
        if((flags & XAResource.TMENDRSCAN) != 0) {
            buff.append("|XAResource.TMENDRSCAN");
        }
        if((flags & XAResource.TMFAIL) != 0) {
            buff.append("|XAResource.TMFAIL");
        }
        if((flags & XAResource.TMJOIN) != 0) {
            buff.append("|XAResource.TMJOIN");
        }
        if((flags & XAResource.TMONEPHASE) != 0) {
            buff.append("|XAResource.TMONEPHASE");
        }
        if((flags & XAResource.TMRESUME) != 0) {
            buff.append("|XAResource.TMRESUME");
        }
        if((flags & XAResource.TMSTARTRSCAN) != 0) {
            buff.append("|XAResource.TMSTARTRSCAN");
        }
        if((flags & XAResource.TMSUCCESS) != 0) {
            buff.append("|XAResource.TMSUCCESS");
        }
        if((flags & XAResource.TMSUSPEND) != 0) {
            buff.append("|XAResource.TMSUSPEND");
        }
        if(buff.length() == 0) {
            buff.append("|XAResource.TMNOFLAGS");
        }
        return buff.toString().substring(1);
    }
    
    private String quoteXid(Xid xid) {
        StringBuffer buff = new StringBuffer();
        buff.append("\"f:");
        buff.append(xid.getFormatId());
        buff.append(",bq:");
        buff.append(ByteUtils.convertBytesToString(xid.getBranchQualifier()));
        buff.append(",gxid:");
        buff.append(ByteUtils.convertBytesToString(xid.getGlobalTransactionId()));
        buff.append(",c:");
        buff.append(xid.getClass().getName());
        buff.append("\"");
        return buff.toString();
    }

    public void start(Xid xid, int flags) throws XAException {
        debugCode("start("+quoteXid(xid)+", "+quoteFlags(flags)+")");
        if(flags == TMRESUME) {
            return;
        }
        if(currentTransaction != null) {
            getTrace().debug("throw XAException.XAER_NOTA");
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            conn.setAutoCommit(false);
        } catch(SQLException e) {
            throw convertException(e);
        }
        getTrace().debug("currentTransaction=xid");
        currentTransaction = xid;
    }
    
    private XAException convertException(SQLException e) {
        getTrace().debug("throw XAException("+e.getMessage()+")");
        return new XAException(e.getMessage());
    }

    public void commit(Xid xid, boolean onePhase) throws XAException {
        debugCode("commit("+quoteXid(xid)+", "+onePhase+")");
        try {
            conn.commit();
        } catch(SQLException e) {
            throw convertException(e);
        }
        getTrace().debug("committed");
    }

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

}
