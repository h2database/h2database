/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.test.db.TestScript;
import org.h2.test.synth.sql.RandomGen;
import org.h2.util.New;
import org.h2.util.RandomUtils;

/**
 * A test that calls random methods with random parameters from JDBC objects.
 * This is sometimes called 'Fuzz Testing'.
 */
public class TestCrashAPI extends TestBase {

    private static final Class< ? >[] INTERFACES = { Connection.class, PreparedStatement.class, Statement.class,
            ResultSet.class, ResultSetMetaData.class, Savepoint.class,
            ParameterMetaData.class, Clob.class, Blob.class, Array.class, CallableStatement.class };

    private static final String DIR = "synth";

    private ArrayList<Object> objects = New.arrayList();
    private HashMap<Class < ? >, ArrayList<Method>> classMethods = New.hashMap();
    private RandomGen random = new RandomGen();
    private ArrayList<String> statements = New.arrayList();
    private int openCount;
    private long callCount;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    private void deleteDb() {
        try {
            deleteDb(baseDir + "/" + DIR, null);
        } catch (Exception e) {
            // ignore
        }
    }

    private Connection getConnection(int seed, boolean delete) throws SQLException {
        openCount++;
        if (delete) {
            deleteDb();
        }
        // can not use FILE_LOCK=NO, otherwise something could be written into
        // the database in the finalize method

        String add = "";

//         int testing;
//        if(openCount >= 32) {
//            int test;
//            Runtime.getRuntime().halt(0);
//            System.exit(1);
//        }
        // add = ";LOG=2";
        // System.out.println("now open " + openCount);
        // add += ";TRACE_LEVEL_FILE=3";
        // config.logMode = 2;
        // }

        String url = getURL(DIR + "/crashApi" + seed, true) + add;

//        int test;
//        url += ";DB_CLOSE_ON_EXIT=FALSE";
//      int test;
//      url += ";TRACE_LEVEL_FILE=3";

        Connection conn = null;
        // System.gc();
        conn = DriverManager.getConnection(url, "sa", getPassword(""));
        int len = random.getInt(50);
        int start = random.getInt(statements.size() - len);
        int end = start + len;
        Statement stat = conn.createStatement();
        stat.execute("SET LOCK_TIMEOUT 10");
        stat.execute("SET WRITE_DELAY 0");
        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                double g = random.nextGaussian();
                int size = (int) Math.abs(10000 * g * g);
                stat.execute("SET CACHE_SIZE " + size);
            } else {
                stat.execute("SET CACHE_SIZE 0");
            }
        }
        stat.execute("SCRIPT NOPASSWORDS NOSETTINGS");
        for (int i = start; i < end && i < statements.size(); i++) {
            try {
                stat.execute("SELECT * FROM TEST WHERE ID=1");
            } catch (Throwable t) {
                printIfBad(seed, -i, -1, t);
            }
            try {
                stat.execute("SELECT * FROM TEST WHERE ID=1 OR ID=1");
            } catch (Throwable t) {
                printIfBad(seed, -i, -1, t);
            }

            String sql = statements.get(i);
            try {
//                if(openCount == 32) {
//                    int test;
//                    System.out.println("stop!");
//                }
                stat.execute(sql);
            } catch (Throwable t) {
                printIfBad(seed, -i, -1, t);
            }
        }
        if (random.nextBoolean()) {
            try {
                conn.commit();
            } catch (Throwable t) {
                printIfBad(seed, 0, -1, t);
            }
        }
        return conn;
    }

    private void testOne(int seed) throws SQLException {
        printTime("seed: " + seed);
        callCount = 0;
        openCount = 0;
        random = new RandomGen();
        random.setSeed(seed);
        Connection c1 = getConnection(seed, true);
        Connection conn = null;
        for (int i = 0; i < 2000; i++) {
            // if(i % 10 == 0) {
            // for(int j=0; j<objects.size(); j++) {
            // System.out.print(objects.get(j));
            // System.out.print(" ");
            // }
            // System.out.println();
            // Thread.sleep(1);
            // }

            if (objects.size() == 0) {
                try {
                    conn = getConnection(seed, false);
                } catch (SQLException e) {
                    if (e.getSQLState().equals("08004")) {
                        // Wrong user/password [08004]
                        try {
                            c1.createStatement().execute("SET PASSWORD ''");
                        } catch (Throwable t) {
                            // power off or so
                            break;
                        }
                        try {
                            conn = getConnection(seed, false);
                        } catch (Throwable t) {
                            printIfBad(seed, -i, -1, t);
                        }
                    } else if (e.getSQLState().equals("90098")) {
                        // The database has been closed
                        break;
                    } else {
                        printIfBad(seed, -i, -1, e);
                    }
                }
                objects.add(conn);
            }
            int objectId = random.getInt(objects.size());
            if (random.getBoolean(1)) {
                objects.remove(objectId);
                continue;
            }
            if (random.getInt(2000) == 0 && conn != null) {
                ((JdbcConnection) conn).setPowerOffCount(random.getInt(50));
            }
            Object o = objects.get(objectId);
            if (o == null) {
                objects.remove(objectId);
                continue;
            }
            Class< ? > in = getJdbcInterface(o);
            ArrayList<Method> methods = classMethods.get(in);
            Method m = methods.get(random.getInt(methods.size()));
            Object o2 = callRandom(seed, i, objectId, o, m);
            if (o2 != null) {
                objects.add(o2);
            }
        }
        try {
            if (conn != null) {
                conn.close();
            }
            c1.close();
        } catch (Throwable t) {
            printIfBad(seed, -101010, -1, t);
            try {
                deleteDb(null, "test");
            } catch (Throwable t2) {
                printIfBad(seed, -101010, -1, t2);
            }
        }
        objects.clear();
    }

    private void printError(int seed, int id, Throwable t) {
        StringWriter writer = new StringWriter();
        t.printStackTrace(new PrintWriter(writer));
        String s = writer.toString();
        TestBase.logError("new TestCrashAPI().init(test).testCase(" +
                seed + "); // Bug " + s.hashCode() + " id=" + id +
                " callCount=" + callCount + " openCount=" + openCount +
                " " + t.getMessage(), t);
    }

    private Object callRandom(int seed, int id, int objectId, Object o, Method m) {
        Class< ? >[] paramClasses = m.getParameterTypes();
        Object[] params = new Object[paramClasses.length];
        for (int i = 0; i < params.length; i++) {
            params[i] = getRandomParam(paramClasses[i]);
        }
        Object result = null;
        try {
            callCount++;
            result = m.invoke(o, params);
        } catch (IllegalArgumentException e) {
            TestBase.logError("error", e);
        } catch (IllegalAccessException e) {
            TestBase.logError("error", e);
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            printIfBad(seed, id, objectId, t);
        }
        if (result == null) {
            return null;
        }
        Class< ? > in = getJdbcInterface(result);
        if (in == null) {
            return null;
        }
        return result;
    }

    private void printIfBad(int seed, int id, int objectId, Throwable t) {
        if (t instanceof BatchUpdateException) {
            // do nothing
        } else if (t.getClass().getName().indexOf("SQLClientInfoException") >= 0) {
            // do nothing
        } else if (t instanceof SQLException) {
            SQLException s = (SQLException) t;
            int errorCode = s.getErrorCode();
            if (errorCode == 0) {
                printError(seed, id, s);
            } else if (errorCode == ErrorCode.OBJECT_CLOSED) {
                if (objectId >= 0) {
                    // TODO at least call a few more times after close - maybe
                    // there is still an error
                    objects.remove(objectId);
                }
            } else if (errorCode == ErrorCode.GENERAL_ERROR_1) {
                // General error [HY000]
                printError(seed, id, s);
            }
        } else {
            printError(seed, id, t);
        }
    }

    private Object getRandomParam(Class< ? > type) {
        if (type == int.class) {
            return new Integer(random.getRandomInt());
        } else if (type == byte.class) {
            return new Byte((byte) random.getRandomInt());
        } else if (type == short.class) {
            return new Short((short) random.getRandomInt());
        } else if (type == long.class) {
            return new Long(random.getRandomLong());
        } else if (type == float.class) {
            return new Float(random.getRandomDouble());
        } else if (type == boolean.class) {
            return new Boolean(random.nextBoolean());
        } else if (type == double.class) {
            return new Double(random.getRandomDouble());
        } else if (type == String.class) {
            if (random.getInt(10) == 0) {
                return null;
            }
            int randomId = random.getInt(statements.size());
            String sql = statements.get(randomId);
            if (random.getInt(10) == 0) {
                sql = random.modify(sql);
            }
            return sql;
        } else if (type == int[].class) {
            // TODO test with 'shared' arrays (make sure database creates a
            // copy)
            return random.getIntArray();
        } else if (type == java.io.Reader.class) {
            return null;
        } else if (type == java.sql.Array.class) {
            return null;
        } else if (type == byte[].class) {
            // TODO test with 'shared' arrays (make sure database creates a
            // copy)
            return random.getByteArray();
        } else if (type == Map.class) {
            return null;
        } else if (type == Object.class) {
            return null;
        } else if (type == java.sql.Date.class) {
            return random.randomDate();
        } else if (type == java.sql.Time.class) {
            return random.randomTime();
        } else if (type == java.sql.Timestamp.class) {
            return random.randomTimestamp();
        } else if (type == java.io.InputStream.class) {
            return null;
        } else if (type == String[].class) {
            return null;
        } else if (type == java.sql.Clob.class) {
            return null;
        } else if (type == java.sql.Blob.class) {
            return null;
        } else if (type == Savepoint.class) {
            // TODO should use generated savepoints
            return null;
        } else if (type == Calendar.class) {
            return Calendar.getInstance();
        } else if (type == java.net.URL.class) {
            return null;
        } else if (type == java.math.BigDecimal.class) {
            return new java.math.BigDecimal("" + random.getRandomDouble());
        } else if (type == java.sql.Ref.class) {
            return null;
        }
        return null;
    }

    private Class< ? > getJdbcInterface(Object o) {
        Class< ? >[] list = o.getClass().getInterfaces();
        for (int i = 0; i < list.length; i++) {
            Class< ? > in = list[i];
            if (classMethods.get(in) != null) {
                return in;
            }
        }
        return null;
    }

    private void initMethods() {
        for (int i = 0; i < INTERFACES.length; i++) {
            Class< ? > inter = INTERFACES[i];
            classMethods.put(inter, new ArrayList<Method>());
        }
        for (int i = 0; i < INTERFACES.length; i++) {
            Class< ? > inter = INTERFACES[i];
            ArrayList<Method> list = classMethods.get(inter);
            Method[] methods = inter.getMethods();
            for (int j = 0; j < methods.length; j++) {
                Method m = methods[j];
                list.add(m);
            }
        }
    }

    public TestBase init(TestAll conf) throws Exception {
        super.init(conf);
        if (config.mvcc || config.networked || config.logMode == 0) {
            return this;
        }
        baseDir = TestBase.getTestDir("crash");
        startServerIfRequired();
        TestScript script = new TestScript();
        ArrayList<String> add = script.getAllStatements(config);
        initMethods();
        org.h2.Driver.load();
        statements.addAll(add);
        return this;
    }

    public void testCase(int i) throws SQLException {
        int old = SysProperties.getMaxQueryTimeout();
        String oldBaseDir = baseDir;
        try {
            System.setProperty(SysProperties.H2_MAX_QUERY_TIMEOUT, "" + 10000);
            baseDir = TestBase.getTestDir("crash");
            testOne(i);
        } finally {
            baseDir = oldBaseDir;
            System.setProperty(SysProperties.H2_MAX_QUERY_TIMEOUT, "" + old);
        }
    }

    public void test() throws SQLException {
        if (config.mvcc || config.networked || config.logMode == 0) {
            return;
        }
        int len = getSize(2, 6);
        for (int i = 0; i < len; i++) {
            int seed = RandomUtils.nextInt(Integer.MAX_VALUE);
            testCase(seed);
            deleteDb();
        }
    }

}
