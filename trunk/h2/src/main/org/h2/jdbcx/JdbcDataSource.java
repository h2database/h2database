/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import org.h2.jdbc.JdbcConnection;
import org.h2.message.TraceObject;

/**
 * A data source for H2 database connections
 * 
 * @author Tom
 */
public class JdbcDataSource extends TraceObject implements XADataSource, DataSource, ConnectionPoolDataSource, Serializable, Referenceable  {
    
    private static final long serialVersionUID = 1288136338451857771L;
    
    private JdbcDataSourceFactory factory;
    private int timeout;
    private PrintWriter logWriter;
    private String user;
    private String password;
    private String url;
    
    public JdbcDataSource() {
        this.factory = new JdbcDataSourceFactory();
        int id = getNextId(TraceObject.DATASOURCE);
        setTrace(factory.getTrace(), TraceObject.DATASOURCE, id);
    }

    public int getLoginTimeout() throws SQLException {
        debugCodeCall("getLoginTimeout");
        return timeout;
    }

    public void setLoginTimeout(int timeout) throws SQLException {
        debugCodeCall("setLoginTimeout", timeout);
        this.timeout = timeout;
    }

    public PrintWriter getLogWriter() throws SQLException {
        debugCodeCall("getLogWriter");
        return logWriter;
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        debugCodeCall("setLogWriter(out)");
        logWriter = out;
    }

    public Connection getConnection() throws SQLException {
        debugCodeCall("getConnection");
        return getJdbcConnection(user, password);
    }

    public Connection getConnection(String user, String password) throws SQLException {
        debugCode("getConnection("+quote(user)+", "+quote(password)+");");
        return getJdbcConnection(user, password);
    }

    public JdbcConnection getJdbcConnection(String user, String password) throws SQLException {
        debugCode("getJdbcConnection("+quote(user)+", "+quote(password)+");");
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        return new JdbcConnection(url, info);
    }
    
    public String getURL() {
        debugCodeCall("getURL");
        return url;
    }

    public void setURL(String url) {
        debugCodeCall("setURL", url);
        this.url = url;
    }

    public String getPassword() {
        debugCodeCall("getPassword");
        return password;
    }

    public void setPassword(String password) {
        debugCodeCall("setPassword", password);
        this.password = password;
    }

    public String getUser() {
        debugCodeCall("getUser");
        return user;
    }

    public void setUser(String user) {
        debugCodeCall("setUser", user);
        this.user = user;
    }
    

    public Reference getReference() throws NamingException {
        debugCodeCall("getReference");
        String factoryClassName = JdbcDataSourceFactory.class.getName();
        Reference ref = new Reference(getClass().getName(), factoryClassName, null);
        ref.add(new StringRefAddr("url", getURL()));
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", password));
        return ref;
    }

    public XAConnection getXAConnection() throws SQLException {
        debugCodeCall("getXAConnection");
        int id = getNextId(XA_DATASOURCE);
        return new JdbcXAConnection(factory, id, url, user, password);
    }

    public XAConnection getXAConnection(String user, String password) throws SQLException {
        debugCode("getXAConnection("+quote(user)+", "+quote(password)+");");
        int id = getNextId(XA_DATASOURCE);
        return new JdbcXAConnection(factory, id, url, user, password);
    }

    public PooledConnection getPooledConnection() throws SQLException {
        debugCodeCall("getPooledConnection");
        return getXAConnection();
    }

    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        debugCode("getPooledConnection("+quote(user)+", "+quote(password)+");");
        return getXAConnection(user, password);
    }    

}
