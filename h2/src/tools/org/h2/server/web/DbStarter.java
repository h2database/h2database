/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import javax.servlet.*;
import java.sql.*;

public class DbStarter implements ServletContextListener {
    
    private Connection conn;

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            org.h2.Driver.load();
            // You can also get the setting from a context-param in web.xml:
            ServletContext servletContext = servletContextEvent.getServletContext();
            // String url = servletContext.getInitParameter("db.url");
            conn = DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
            servletContext.setAttribute("connection", conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public Connection getConnection() {
        return conn;
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        try {
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
