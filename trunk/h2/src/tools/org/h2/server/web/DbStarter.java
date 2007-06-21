package org.h2.server.web;

import javax.servlet.*;
import java.sql.*;

public class DbStarter implements ServletContextListener {
	
    private Connection conn;

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            Class.forName("org.h2.Driver");
            // You can also get the setting from a context-param in web.xml:
            ServletContext servletContext = servletContextEvent.getServletContext();
            // String url = servletContext.getInitParameter("db.url");
            conn = DriverManager.getConnection("jdbc:h2:test", "sa", "");
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
