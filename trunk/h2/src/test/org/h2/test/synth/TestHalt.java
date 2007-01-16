package org.h2.test.synth;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.IOUtils;

public abstract class TestHalt extends TestBase {
    
    private SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm:ss ");
    protected static final int OP_INSERT = 1, OP_DELETE = 2, OP_UPDATE = 4, OP_SELECT = 8;
    protected static final int FLAG_NODELAY = 1, FLAG_LOBS = 2;
    protected int operations, flags, value;
    protected Connection conn;
    protected Random random = new Random();
    
    abstract void testInit() throws Exception;
    abstract void testCheckAfterCrash() throws Exception;
    abstract void testWaitAfterAppStart() throws Exception;
    abstract void appStart() throws Exception;
    abstract void appRun() throws Exception;

    public void test() throws Exception {
        for(int i=0;; i++) {
            operations = OP_INSERT | i;
            flags = i >> 4;
            // flags = FLAG_NODELAY | FLAG_LOBS;
            try {
                runTest();
            } catch(Throwable t) {
                System.out.println("Error: " + t);
                t.printStackTrace();
            }
        }
    }
    
    Connection getConnection() throws Exception {
        Class.forName("org.h2.Driver");
        return DriverManager.getConnection("jdbc:h2:test", "sa", "sa");
    }
    
    protected void start(String[] args) throws Exception {
        if(args.length == 0) {
            runTest();
        } else {
            operations = Integer.parseInt(args[0]);
            flags = Integer.parseInt(args[1]);
            value = Integer.parseInt(args[2]);
            runRandom();
        }
    }
    
    private void runRandom() throws Exception {
        log("connecting", null);
        connect();
        try {
            log("connected, operations:" + operations + " flags:" + flags + " value:" + value, null);
            appStart();
            System.out.println("READY");
            System.out.println("READY");
            System.out.println("READY");
            appRun();
            log("done", null);
        } catch(Exception e) {
            log("run", e);
        }
        disconnect();
    }
    
    private void connect() throws Exception {
        try {
            conn = getConnection();
        } catch(Exception e) {
            log("connect", e);
            e.printStackTrace();
            throw e;
        }
    }
    
    protected void log(String s, Exception e) {
        FileWriter writer = null;
        try {
            writer = new FileWriter("log.txt", true);
            PrintWriter w = new PrintWriter(writer);
            s = dateFormat.format(new Date()) + ": " + s;
            w.println(s);
            if(e != null) {
                e.printStackTrace(w);
            }
        } catch(IOException e2) {
            e2.printStackTrace();
        } finally {
            IOUtils.closeSilently(writer);
        }
    }
    
    private void runTest() throws Exception {
        DeleteDbFiles.execute(null, "test", true);
        new File("log.txt").delete();
        connect();
        testInit();
        disconnect();
        for(int i=0; i<10; i++) {
            // int operations = OP_INSERT;
            // OP_DELETE = 1, OP_UPDATE = 2, OP_SELECT = 4;
            // int flags = FLAG_NODELAY;
            // FLAG_NODELAY = 1, FLAG_AUTOCOMMIT = 2, FLAG_SMALLCACHE = 4;
            int value = random.nextInt(1000);
            // for Derby and HSQLDB
            // String classPath = "-cp .;D:/data/java/hsqldb.jar;D:/data/java/derby.jar";
            String classPath = "";
            String command = "java " + classPath + " " + getClass().getName() + " " + operations + " " + flags + " " + value;
            log("start: " + command);
            Process p = Runtime.getRuntime().exec(command);
            InputStream in = p.getInputStream();
            OutputCatcher catcher = new OutputCatcher(in);
            catcher.start();
            String s = catcher.readLine(5000);
            if(s == null) {
                throw new IOException("No reply from process");
            } else if(s.startsWith("READY")) {
                log("got reply: " + s);
            }
            testWaitAfterAppStart();
            p.destroy();
            connect();
            testCheckAfterCrash();
            disconnect();
        }
    }

    protected void disconnect() {
        try {
            conn.close();
        } catch(Exception e) {
            log("disconnect", e);
        }
    }

    private void log(String string) {
        System.out.println(string);
    }
    
    private static class OutputCatcher extends Thread {
        private InputStream in;
        private LinkedList list = new LinkedList();
        
        OutputCatcher(InputStream in) {
            this.in = in;
        }
        
        private String readLine(long wait) {
            long start = System.currentTimeMillis();
            while(true) {
                synchronized(list) {
                    if(list.size() > 0) {
                        return (String) list.removeFirst();
                    }
                    try {
                        list.wait(wait);
                    } catch (InterruptedException e) {
                    }
                    long time = System.currentTimeMillis() - start;
                    if(time >= wait) {
                        return null;
                    }
                }
            }
        }
        
        public void run() {
            StringBuffer buff = new StringBuffer();
            while(true) {
                try {
                    int x = in.read();
                    if(x < 0) {
                        break;
                    }
                    if(x < ' ') {
                        if(buff.length() > 0) {
                            String s = buff.toString();
                            buff.setLength(0);
                            synchronized(list) {
                                list.add(s);
                                list.notifyAll();
                            }
                        }
                    } else {
                        buff.append((char) x);
                    }
                } catch(IOException e) {
                    // ignore
                }
            }
            IOUtils.closeSilently(in);
        }
    }
    
    public Connection getConnectionHSQLDB() throws Exception {
        File lock = new File("test.lck");
        while(lock.exists()) {
            lock.delete();
            System.gc();
        }
        Class.forName("org.hsqldb.jdbcDriver");
        return DriverManager.getConnection("jdbc:hsqldb:test", "sa", "");
    }
    
    public Connection getConnectionDerby() throws Exception {
        File lock = new File("test3/db.lck");
        while(lock.exists()) {
            lock.delete();
            System.gc();
        }
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        try {
            return DriverManager.getConnection("jdbc:derby:test3;create=true", "sa", "sa");
        } catch(SQLException e) {
            Exception e2 = e;
            do {
                e.printStackTrace();
                e = e.getNextException();
            } while(e != null);
            throw e2;
        }
    }
    
    public void disconnectHSQLDB() {
        try {
            conn.createStatement().execute("SHUTDOWN");
        } catch(Exception e) {
            // ignore
        }
        // super.disconnect();
    }
        
    public void disconnectDerby() {
        // super.disconnect();
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            DriverManager.getConnection("jdbc:derby:;shutdown=true", "sa", "sa");
        } catch(Exception e) {
            // ignore
        }
    }

    protected String getRandomString(int len) {
        StringBuffer buff = new StringBuffer();
        for(int i=0; i<len; i++) {
            buff.append('a' + random.nextInt(20));
        }
        return buff.toString();
    }

}
