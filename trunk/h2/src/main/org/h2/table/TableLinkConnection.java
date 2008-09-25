/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import org.h2.constant.SysProperties;
import org.h2.util.JdbcUtils;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;

/**
 * A connection for a linked table. The same connection may be used for multiple
 * tables, that means a connection may be shared.
 */
public class TableLinkConnection {

    /**
     * The map where the link is kept.
     */
    private HashMap map;
    
    /**
     * The connection information.
     */
    private final String driver, url, user, password;
    
    /**
     * The database connection.
     */
    private Connection conn;
    
    /**
     * How many times the connection is used.
     */
    private int useCounter;
    
    public static TableLinkConnection open(HashMap map, String driver, String url, String user, String password) throws SQLException {
        TableLinkConnection t = new TableLinkConnection(map, driver, url, user, password);
        if (SysProperties.SHARE_LINKED_CONNECTIONS) {
            t.open();
            return t;
        }
        synchronized (map) {
            TableLinkConnection result;
            result = (TableLinkConnection) map.get(t);
            if (result == null) {
                synchronized (t) {
                    t.open();
                }
                // put the connection in the map after is has been opened,
                // so we know it works
                map.put(t, t);
                result = t;
            }
            synchronized (result) {
                result.useCounter++;
            }
            return result;
        }
    }
    
    private TableLinkConnection(HashMap map, String driver, String url, String user, String password) {
        this.map = map;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }
    
    private void open() throws SQLException {
        conn = JdbcUtils.getConnection(driver, url, user, password);
    }
    
    public int hashCode() {
        return ObjectUtils.hashCode(driver) 
                ^ ObjectUtils.hashCode(url) 
                ^ ObjectUtils.hashCode(user)
                ^ ObjectUtils.hashCode(password);
    }
    
    public boolean equals(Object o) {
        if (o instanceof TableLinkConnection) {
            TableLinkConnection other = (TableLinkConnection) o;
            return StringUtils.equals(driver, other.driver) 
                    && StringUtils.equals(url, other.url)
                    && StringUtils.equals(user, other.user) 
                    && StringUtils.equals(password, other.password);
        }
        return false;
    }
    
    /**
     * Get the connection. 
     * This method and methods on the statement must be
     * synchronized on this object.
     * 
     * @return the connection
     */
    Connection getConnection() {
        return conn;
    }

    /**
     * Closes the connection if this is the last link to it.
     */
    synchronized void close() throws SQLException {
        if (--useCounter <= 0) {
            conn.close();
            conn = null;
            synchronized (map) {
                map.remove(this);
            }
        }        
    }

}
