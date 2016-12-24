package org.h2.test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Bug_H2_1 {

    static String dbUrl = "jdbc:h2:nioMemFS:dbbug1;DB_CLOSE_DELAY=-1;MULTI_THREADED=1";
    static String user = "sa", pwd = "";
    static int threadCount = 100;

    public static void main(String args[]) throws InterruptedException, SQLException {
        Arte2[] artes = new Arte2[threadCount];
        Connection dbConnect;
        try {
            Class.forName("org.h2.Driver");
            dbConnect = DriverManager.getConnection(dbUrl, user, pwd);
            DbPreparator2.prepareScheme(dbConnect);
            System.out.println("DB scheme prepared");

            DbPreparator2.populate(dbConnect);
            System.out.println("DB populated");

            for (int i = 0; i < threadCount; i++) {
                artes[i] = new Arte2(DriverManager.getConnection(dbUrl, user, pwd));
            }
            System.out.println("ARTEs created");
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("DB Connection Failed: " + dbUrl);
            e.printStackTrace();
            return;
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threadPool.submit(artes[i]);
        }

        while (true) {
            Thread.sleep(100);
        }
    }
}

class DbPreparator2 {

    public static final int OBJ_CNT = 10000;
    private static final String[] SQLs = new String[] { "CREATE TABLE IF NOT EXISTS ACCOUNT ("
            + " ID NUMBER(18,0) not null PRIMARY KEY, BALANCE NUMBER null)" };

    public static void prepareScheme(Connection db) throws SQLException {
        for (String sql : SQLs) {
            db.createStatement().execute(sql);
            db.commit();
        }
    }

    public static void populate(Connection db) throws SQLException {
        PreparedStatement mergeAcctStmt = db.prepareStatement("MERGE INTO Account(id, balance) key (id) VALUES (?, ?)");
        for (int i = 0; i < OBJ_CNT; i++) {
            mergeAcctStmt.setLong(1, i);
            mergeAcctStmt.setBigDecimal(2, BigDecimal.ZERO);
            mergeAcctStmt.addBatch();
        }
        mergeAcctStmt.executeBatch();
        db.commit();
    }
}

class Arte2 implements Callable<Integer> {

    Connection db;
    PreparedStatement updateAcctStmt;

    public Arte2(Connection db_) throws SQLException {
        db = db_;
        db.setAutoCommit(false);
        updateAcctStmt = db.prepareStatement("UPDATE account set balance = ? where id = ?");
    }

    @Override
    public Integer call() {
        try {
            while (true) {
                updateAcctStmt.setDouble(1, Math.random());
                updateAcctStmt.setLong(2, (int) (Math.random() * DbPreparator2.OBJ_CNT));
                updateAcctStmt.execute();
                db.commit();
            }
        } catch (SQLException e) {
            System.out.println("DB write error: ");
            e.printStackTrace();
            System.exit(0);
        }
        return null;
    }
}
