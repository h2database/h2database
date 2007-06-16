/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.message.TraceSystem;
import org.h2.util.FileUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;

public class AppServer {

    private static final String[] GENERIC = new String[] {
        "Generic Firebird Server|org.firebirdsql.jdbc.FBDriver|jdbc:firebirdsql:localhost:c:/temp/firebird/test|sysdba",
        "Generic OneDollarDB|in.co.daffodil.db.jdbc.DaffodilDBDriver|jdbc:daffodilDB_embedded:school;path=C:/temp;create=true|sa",
        "Generic DB2|COM.ibm.db2.jdbc.net.DB2Driver|jdbc:db2://<host>/<db>|" ,
        "Generic Oracle|oracle.jdbc.driver.OracleDriver|jdbc:oracle:thin:@<host>:1521:<instance>|scott" ,
        "Generic PostgreSQL|org.postgresql.Driver|jdbc:postgresql:<db>|" ,
        "Generic MS SQL Server|com.microsoft.jdbc.sqlserver.SQLServerDriver|jdbc:Microsoft:sqlserver://localhost:1433;DatabaseName=sqlexpress|sa",
        "Generic MS SQL Server 2005|com.microsoft.sqlserver.jdbc.SQLServerDriver|jdbc:sqlserver://localhost;DatabaseName=test|sa",
        "Generic MySQL|com.mysql.jdbc.Driver|jdbc:mysql://<host>:<port>/<db>|" ,
        "Generic Derby (Embedded)|org.apache.derby.jdbc.EmbeddedDriver|jdbc:derby:test;create=true|sa",
        "Generic Derby (Server)|org.apache.derby.jdbc.ClientDriver|jdbc:derby://localhost:1527/test;create=true|sa",
        "Generic HSQLDB|org.hsqldb.jdbcDriver|jdbc:hsqldb:test;hsqldb.default_table_type=cached|sa" ,
        "Generic H2|org.h2.Driver|jdbc:h2:test|sa",
    };

    // private URLClassLoader urlClassLoader;
    private String driverList;
    private static int ticker;
    private int port;
    private boolean allowOthers;
    private boolean ssl;
    private HashMap connInfoMap = new HashMap();

    AppServer(String[] args) {
        Properties prop = loadProperties();
        driverList = prop.getProperty("drivers");
        port = FileUtils.getIntProperty(prop, "webPort", Constants.DEFAULT_HTTP_PORT);
        ssl = FileUtils.getBooleanProperty(prop, "webSSL", Constants.DEFAULT_HTTP_SSL);
        allowOthers = FileUtils.getBooleanProperty(prop, "webAllowOthers", Constants.DEFAULT_HTTP_ALLOW_OTHERS);
        for(int i=0; args != null && i<args.length; i++) {
            if("-webPort".equals(args[i])) {
                port = MathUtils.decodeInt(args[++i]);
            } else  if("-webSSL".equals(args[i])) {
                ssl = Boolean.valueOf(args[++i]).booleanValue();
            } else  if("-webAllowOthers".equals(args[i])) {
                allowOthers = Boolean.valueOf(args[++i]).booleanValue();
            // } else if("-baseDir".equals(args[i])) {
            //    String baseDir = args[++i];
            }
        }
        // TODO gcj: don't load drivers in case of GCJ
//        if(false) {
//            if(driverList != null) {
//                try {
//                    String[] drivers = StringUtils.arraySplit(driverList, ',', false);
//                    URL[] urls = new URL[drivers.length];
//                    for(int i=0; i<drivers.length; i++) {
//                        urls[i] = new URL(drivers[i]);
//                    }
//                    urlClassLoader = URLClassLoader.newInstance(urls);
//                } catch (MalformedURLException e) {
//                    TraceSystem.traceThrowable(e);
//                }
//            }
//        }
    }

    void setAllowOthers(boolean b) {
        allowOthers = b;
    }

    void setSSL(boolean b) {
        ssl = b;
    }

    void setPort(int port) {
        this.port = port;
    }

    boolean getAllowOthers() {
        return allowOthers;
    }

    boolean getSSL() {
        return ssl;
    }

    int getPort() {
        return port;
    }

    ConnectionInfo getSetting(String name) {
        return (ConnectionInfo)connInfoMap.get(name);
    }

    void updateSetting(ConnectionInfo info) {
        connInfoMap.put(info.name, info);
        info.lastAccess = ticker++;
    }

    void removeSetting(String name) {
        connInfoMap.remove(name);
    }

    private String getPropertiesFileName() {
        // store the properties in the user directory
        return FileUtils.getFileInUserHome(Constants.SERVER_PROPERTIES_FILE);
    }

    Properties loadProperties() {
        String fileName = getPropertiesFileName();
        try {
            return FileUtils.loadProperties(fileName);
        } catch(IOException e) {
            // TODO log exception
            return new Properties();
        }
    }

    String[] getSettingNames() {
        ArrayList list = getSettings();
        String[] names = new String[list.size()];
        for(int i=0; i<list.size(); i++) {
            names[i] = ((ConnectionInfo)list.get(i)).name;
        }
        return names;
    }

    synchronized ArrayList getSettings() {
        ArrayList settings = new ArrayList();
        if(connInfoMap.size() == 0) {
            Properties prop = loadProperties();
            if(prop.size() == 0) {
                for(int i=0; i<AppServer.GENERIC.length; i++) {
                    ConnectionInfo info = new ConnectionInfo(AppServer.GENERIC[i]);
                    settings.add(info);
                    updateSetting(info);
                }
            } else {
                for(int i=0; ; i++) {
                    String data = prop.getProperty(String.valueOf(i));
                    if(data == null) {
                        break;
                    }
                    ConnectionInfo info = new ConnectionInfo(data);
                    settings.add(info);
                    updateSetting(info);
                }
            }
        } else {
            settings.addAll(connInfoMap.values());
        }
        sortConnectionInfo(settings);
        return settings;
    }

    void sortConnectionInfo(ArrayList list) {
          for (int i = 1, j; i < list.size(); i++) {
              ConnectionInfo t = (ConnectionInfo) list.get(i);
              for (j = i - 1; j >= 0 && (((ConnectionInfo)list.get(j)).lastAccess < t.lastAccess); j--) {
                  list.set(j + 1, list.get(j));
              }
              list.set(j + 1, t);
          }
    }

    synchronized void saveSettings() {
        try {
            Properties prop = new Properties();
            if(driverList != null) {
                prop.setProperty("drivers", driverList);
            }
            prop.setProperty("webPort", String.valueOf(port));
            prop.setProperty("webAllowOthers", String.valueOf(allowOthers));
            prop.setProperty("webSSL", String.valueOf(ssl));
            ArrayList settings = getSettings();
            int len = settings.size();
            for(int i=0; i<len; i++) {
                ConnectionInfo info = (ConnectionInfo) settings.get(i);
                if(info != null) {
                    prop.setProperty(String.valueOf(len - i - 1), info.getString());
                }
            }
            OutputStream out = FileUtils.openFileOutputStream(getPropertiesFileName());
            prop.store(out, Constants.SERVER_PROPERTIES_TITLE);
            out.close();
        } catch(Exception e) {
            TraceSystem.traceThrowable(e);
        }
    }

    // TODO GCJ: if this method is synchronized, then the .exe file fails (probably does not unlock the object)
    // and cannot go in here after a class was not found
    Connection getConnection(String driver, String url, String user, String password) throws Exception {
        driver = driver.trim();
        url = url.trim();
        user = user.trim();
        password = password.trim();
        org.h2.Driver.load();
//            try {
//                Driver dr = (Driver) urlClassLoader.loadClass(driver).newInstance();
//                Properties p = new Properties();
//                p.setProperty("user", user);
//                p.setProperty("password", password);
//                return dr.connect(url, p);
//            } catch(ClassNotFoundException e2) {
//                throw e2;
//            }
        return JdbcUtils.getConnection(driver, url, user, password);
    }

}
