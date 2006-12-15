/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;

import org.h2.test.TestBase;
import org.h2.tools.Server;
import org.h2.util.StringUtils;

class Database {
    
    private TestPerformance test;
    private int id;
    private String name, url, user, password;
    private ArrayList replace = new ArrayList();
    private String action;
    private long startTime;
    private Connection conn;
    private Statement stat;
    private boolean trace = true;
    private long lastTrace;
    private Random random = new Random(1);
    private ArrayList results = new ArrayList();
    private int totalTime;
    private int executedStatements;
    
    private Server serverH2;
    private Object serverDerby;
    private boolean serverHSQLDB;
    
    String getName() {
        return name;
    }
    
    int getTotalTime() {
        return totalTime;
    }
    
    ArrayList getResults() {
        return results;
    }
    
    Random getRandom() {
        return random;
    }
    
    void startServer() throws Exception {
        if(url.startsWith("jdbc:h2:tcp:")) {
            serverH2 = Server.createTcpServer(new String[0]).start();
            Thread.sleep(100);
        } else if(url.startsWith("jdbc:derby://")) {
            serverDerby = Class.forName("org.apache.derby.drda.NetworkServerControl").newInstance();
            Method m = serverDerby.getClass().getMethod("start", new Class[]{PrintWriter.class});
            m.invoke(serverDerby, new Object[]{null});
            // serverDerby = new NetworkServerControl();
            // serverDerby.start(null);
            Thread.sleep(100);
        } else if(url.startsWith("jdbc:hsqldb:hsql:")) {
            if(!serverHSQLDB) {
                Class c = Class.forName("org.hsqldb.Server");
                Method m = c.getMethod("main", new Class[]{String[].class});
                m.invoke(null, new Object[]{new String[]{"-database.0", "data/mydb;hsqldb.default_table_type=cached", "-dbname.0", "xdb"}});
                // org.hsqldb.Server.main(new String[]{"-database.0", "mydb", "-dbname.0", "xdb"});
                serverHSQLDB = true;
                Thread.sleep(100);
            }
        }
    }
    
    void stopServer() throws Exception {
        if(serverH2 != null) {
            serverH2.stop();
            serverH2 = null;
        }
        if(serverDerby != null) {
            Method m = serverDerby.getClass().getMethod("shutdown", new Class[]{});
            m.invoke(serverDerby, null);
            // serverDerby.shutdown();
            serverDerby = null;
        } else if(serverHSQLDB) {
            // can not shut down (shutdown calls System.exit)
            // openConnection();
            // update("SHUTDOWN");
            // closeConnection();
            // serverHSQLDB = false;
        }
    }
    
    static Database parse(TestPerformance test, int id, String dbString) {
        try {
            StringTokenizer tokenizer = new StringTokenizer(dbString, ",");
            Database db = new Database();
            db.id = id;
            db.test = test;
            db.name = tokenizer.nextToken().trim();
            String driver = tokenizer.nextToken().trim();
            Class.forName(driver);
            db.url = tokenizer.nextToken().trim();
            db.user = tokenizer.nextToken().trim();
            db.password = "";
            if(tokenizer.hasMoreTokens()) {
                db.password = tokenizer.nextToken().trim();
            }
            System.out.println("Loaded successfully: " + db.name);
            return db;
        } catch(Exception e) {
            System.out.println("Cannot load database " + dbString +" :" + e.toString());
            return null;
        }
    }
    
    Connection getConnection() throws Exception {
        Connection conn = DriverManager.getConnection(url, user, password);
        if(url.startsWith("jdbc:derby:")) {
            // Derby: use higher cache size
            conn.createStatement().execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '8192')");
        }
        return conn;
    }
    
    void openConnection() throws Exception {
        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();
    }
    
    void closeConnection() throws Exception {
//        if(!serverHSQLDB && url.startsWith("jdbc:hsqldb:")) {
//            stat.execute("SHUTDOWN");
//        }
        conn.close();
        stat = null;
        conn = null;
    }

    public void setTranslations(Properties prop) {
        String id = url.substring("jdbc:".length());
        id = id.substring(0, id.indexOf(':'));
        for(Iterator it = prop.keySet().iterator(); it.hasNext(); ) {
            String key = (String)it.next();
            if(key.startsWith(id + ".")) {
                String pattern = key.substring(id.length()+1);
                pattern = StringUtils.replaceAll(pattern, "_", " ");
                pattern = StringUtils.toUpperEnglish(pattern);
                String replacement = prop.getProperty(key);
                replace.add(new String[]{pattern, replacement});
            }
        }
    }
    
    PreparedStatement prepare(String sql) throws Exception {
        sql = getSQL(sql);
        return conn.prepareStatement(sql);
    }
    
    public String getSQL(String sql) {
        for(int i=0; i<replace.size(); i++) {
            String[] pair = (String[]) replace.get(i);
            String pattern = pair[0];
            String replace = pair[1];
            sql = StringUtils.replaceAll(sql, pattern, replace);
        }
        return sql;
    }
    
    void start(Bench bench, String action) {
        this.action = bench.getName() + ": " + action;
        this.startTime = System.currentTimeMillis();
    }
    
    void end() {
        long time = System.currentTimeMillis() - startTime;
        log(action, "ms", (int)time);
        if(test.collect) {
            totalTime += time;
        }
    }

    void dropTable(String table) {
        try {
            update("DROP TABLE " + table);
        } catch (Exception e) {
            // ignore - table may not exist
        }
    }
    
    public void update(PreparedStatement prep) throws Exception {
        prep.executeUpdate();
        executedStatements++;
    }

    public void update(String sql) throws Exception {
        sql = getSQL(sql);
        if(sql.trim().length()>0) {
            stat.execute(sql);
        } else {
            System.out.println("?");
        }
        executedStatements++;        
    }

    public void setAutoCommit(boolean b) throws Exception {
        conn.setAutoCommit(b);
    }
    
    public void commit() throws Exception {
        conn.commit(); 
    }

    public void rollback() throws Exception {
        conn.rollback();
    }

    void trace(String action, int i, int max) {
      if (trace) {
            long time = System.currentTimeMillis();
            if (i == 0 || lastTrace == 0) {
                lastTrace = time;
            } else if (time > lastTrace + 1000) {
                System.out.println(action + ": " + ((100 * i / max) + "%"));
                lastTrace = time;
            }
        }
    }
    
    void logMemory(Bench bench, String action) {
        log(bench.getName() + ": " + action, "MB", TestBase.getMemoryUsed());
    }
    
    void log(String action, String scale, int value) {
        if(test.collect) {
            results.add(new Object[]{action, scale, new Integer(value)});
        }
    }

    public ResultSet query(PreparedStatement prep) throws Exception {
//        long time = System.currentTimeMillis();
        ResultSet rs = prep.executeQuery();
//        time = System.currentTimeMillis() - time;
//        if(time > 100) {
//            new Error("time="+time).printStackTrace();
//        }
        executedStatements++;
        return rs;
    }

    public void queryReadResult(PreparedStatement prep) throws Exception {
        ResultSet rs = prep.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        while(rs.next()) {
            for(int i=0; i<columnCount; i++) {
                rs.getString(i+1);
            }
        }
    }
    
    int getExecutedStatements() {
        return executedStatements;
    }

    public int getId() {
        return id;
    }

}
