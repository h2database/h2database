/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

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

/*## Java 1.6 begin ##
import org.h2.message.Message;
## Java 1.6 end ##*/
import org.h2.util.StringUtils;

/**
 * A data source for H2 database connections. It is a factory for XAConnection
 * and Connection objects. This class is usually registered in a JNDI naming
 * service. To create a data source object and register it with a JNDI service,
 * use the following code:
 *
 * <pre>
 * import org.h2.jdbcx.JdbcDataSource;
 * import javax.naming.Context;
 * import javax.naming.InitialContext;
 * JdbcDataSource ds = new JdbcDataSource();
 * ds.setURL(&quot;jdbc:h2:&tilde;/test&quot;);
 * ds.setUser(&quot;sa&quot;);
 * ds.setPassword(&quot;sa&quot;);
 * Context ctx = new InitialContext();
 * ctx.bind(&quot;jdbc/dsName&quot;, ds);
 * </pre>
 *
 * To use a data source that is already registered, use the following code:
 *
 * <pre>
 * import java.sql.Connection;
 * import javax.sql.DataSource;
 * import javax.naming.Context;
 * import javax.naming.InitialContext;
 * Context ctx = new InitialContext();
 * DataSource ds = (DataSource) ctx.lookup(&quot;jdbc/dsName&quot;);
 * Connection conn = ds.getConnection();
 * </pre>
 *
 * In this example the user name and password are serialized as
 * well; this may be a security problem in some cases.
 */
public class JdbcDataSource extends TraceObject
//## Java 1.4 begin ##
implements XADataSource, DataSource, ConnectionPoolDataSource, Serializable, Referenceable
//## Java 1.4 end ##
{

    private static final long serialVersionUID = 1288136338451857771L;

    private transient JdbcDataSourceFactory factory;
    private transient PrintWriter logWriter;
    private int loginTimeout;
    private String user = "";
    private char[] password = new char[0];
    private String url = "";

    static {
        org.h2.Driver.load();
    }

    /**
     * The public constructor.
     */
    public JdbcDataSource() {
        initFactory();
        int id = getNextId(TraceObject.DATA_SOURCE);
        setTrace(factory.getTrace(), TraceObject.DATA_SOURCE, id);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        initFactory();
        in.defaultReadObject();
    }

    private void initFactory() {
        factory = new JdbcDataSourceFactory();
    }

    /**
     * Get the login timeout in seconds, 0 meaning no timeout.
     *
     * @return the timeout in seconds
     */
    public int getLoginTimeout() {
        debugCodeCall("getLoginTimeout");
        return loginTimeout;
    }

    /**
     * Set the login timeout in seconds, 0 meaning no timeout.
     * The default value is 0.
     * This value is ignored by this database.
     *
     * @param timeout the timeout in seconds
     */
    public void setLoginTimeout(int timeout) {
        debugCodeCall("setLoginTimeout", timeout);
        this.loginTimeout = timeout;
    }

    /**
     * Get the current log writer for this object.
     *
     * @return the log writer
     */
    public PrintWriter getLogWriter() {
        debugCodeCall("getLogWriter");
        return logWriter;
    }

    /**
     * Set the current log writer for this object.
     * This value is ignored by this database.
     *
     * @param out the log writer
     */
    public void setLogWriter(PrintWriter out) {
        debugCodeCall("setLogWriter(out)");
        logWriter = out;
    }

    /**
     * Open a new connection using the current URL, user name and password.
     *
     * @return the connection
     */
    public Connection getConnection() throws SQLException {
        debugCodeCall("getConnection");
        return getJdbcConnection(user, StringUtils.cloneCharArray(password));
    }

    /**
     * Open a new connection using the current URL and the specified user name
     * and password.
     *
     * @param user the user name
     * @param password the password
     * @return the connection
     */
    public Connection getConnection(String user, String password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getConnection("+quote(user)+", \"\");");
        }
        return getJdbcConnection(user, convertToCharArray(password));
    }

    private JdbcConnection getJdbcConnection(String user, char[] password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getJdbcConnection("+quote(user)+", new char[0]);");
        }
        Properties info = new Properties();
        info.setProperty("user", user);
        info.put("password", password);
        return new JdbcConnection(url, info);
    }

    /**
     * Get the current URL.
     *
     * @return the URL
     */
    public String getURL() {
        debugCodeCall("getURL");
        return url;
    }

    /**
     * Set the current URL.
     *
     * @param url the new URL
     */
    public void setURL(String url) {
        debugCodeCall("setURL", url);
        this.url = url;
    }

    /**
     * Set the current password.
     *
     * @param password the new password.
     */
    public void setPassword(String password) {
        debugCodeCall("setPassword", "");
        this.password = convertToCharArray(password);
    }

    /**
     * Set the current password in the form of a char array.
     *
     * @param password the new password in the form of a char array.
     */
    public void setPasswordChars(char[] password) {
        if (isDebugEnabled()) {
            debugCode("setPasswordChars(new char[0]);");
        }
        this.password = password;
    }

    private char[] convertToCharArray(String s) {
        return s == null ? null : s.toCharArray();
    }

    private String convertToString(char[] a) {
        return a == null ? null : new String(a);
    }

    /**
     * Get the current password.
     *
     * @return the password
     */
    public String getPassword() {
        debugCodeCall("getPassword");
        return convertToString(password);
    }

    /**
     * Get the current user name.
     *
     * @return the user name
     */
    public String getUser() {
        debugCodeCall("getUser");
        return user;
    }

    /**
     * Set the current user name.
     *
     * @param user the new user name
     */
    public void setUser(String user) {
        debugCodeCall("setUser", user);
        this.user = user;
    }

    /**
     * Get a new reference for this object, using the current settings.
     *
     * @return the new reference
     */
//## Java 1.4 begin ##
    public Reference getReference() {
        debugCodeCall("getReference");
        String factoryClassName = JdbcDataSourceFactory.class.getName();
        Reference ref = new Reference(getClass().getName(), factoryClassName, null);
        ref.add(new StringRefAddr("url", url));
        ref.add(new StringRefAddr("user", user));
        ref.add(new StringRefAddr("password", convertToString(password)));
        ref.add(new StringRefAddr("loginTimeout", String.valueOf(loginTimeout)));
        return ref;
    }
//## Java 1.4 end ##

    /**
     * Open a new XA connection using the current URL, user name and password.
     *
     * @return the connection
     */
//## Java 1.4 begin ##
    public XAConnection getXAConnection() throws SQLException {
        debugCodeCall("getXAConnection");
        int id = getNextId(XA_DATA_SOURCE);
        return new JdbcXAConnection(factory, id, url, user, password);
    }
//## Java 1.4 end ##

    /**
     * Open a new XA connection using the current URL and the specified user
     * name and password.
     *
     * @param user the user name
     * @param password the password
     * @return the connection
     */
//## Java 1.4 begin ##
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getXAConnection("+quote(user)+", \"\");");
        }
        int id = getNextId(XA_DATA_SOURCE);
        return new JdbcXAConnection(factory, id, url, user, convertToCharArray(password));
    }
//## Java 1.4 end ##

    /**
     * Open a new pooled connection using the current URL, user name and password.
     *
     * @return the connection
     */
//## Java 1.4 begin ##
    public PooledConnection getPooledConnection() throws SQLException {
        debugCodeCall("getPooledConnection");
        return getXAConnection();
    }
//## Java 1.4 end ##

    /**
     * Open a new pooled connection using the current URL and the specified user
     * name and password.
     *
     * @param user the user name
     * @param password the password
     * @return the connection
     */
//## Java 1.4 begin ##
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getPooledConnection("+quote(user)+", \"\");");
        }
        return getXAConnection(user, password);
    }
//## Java 1.4 end ##

    /**
     * [Not supported] Return an object of this class if possible.
     *
     * @param iface the class
     */
/*## Java 1.6 begin ##
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw Message.getUnsupportedException("unwrap");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     */
/*## Java 1.6 begin ##
    public boolean isWrapperFor(Class< ? > iface) throws SQLException {
        throw Message.getUnsupportedException("isWrapperFor");
    }
## Java 1.6 end ##*/

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": url=" + url + " user=" + user;
    }

}
