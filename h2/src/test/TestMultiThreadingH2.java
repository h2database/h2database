import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestMultiThreadingH2 {

    private static final String JDBC_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MULTI_THREADED=1;";

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");

        // create some common tables and views
        final Connection conn = DriverManager.getConnection(JDBC_URL, "sa", "");
        final Statement stat = conn.createStatement();
        stat.execute(
                "CREATE TABLE INVOICE(INVOICE_ID INT PRIMARY KEY, AMOUNT DECIMAL)");
        stat.execute("CREATE VIEW INVOICE_VIEW as SELECT * FROM INVOICE");

        stat.execute(
                "CREATE TABLE INVOICE_DETAIL(DETAIL_ID INT PRIMARY KEY, INVOICE_ID INT, DESCRIPTION VARCHAR)");
        stat.execute(
                "CREATE VIEW INVOICE_DETAIL_VIEW as SELECT * FROM INVOICE_DETAIL");

        stat.close();
        conn.close();

        // create views that reference the common views in different threads
        ExecutorService executor = Executors.newFixedThreadPool(8);
        for (int i = 0; i < 30000; i++) {
            final int j = i;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection conn2 = DriverManager.getConnection(JDBC_URL,
                                "sa", "");
                        Statement stat2 = conn2.createStatement();

                        stat2.execute("CREATE VIEW INVOICE_VIEW" + j
                                + " as SELECT * FROM INVOICE_VIEW");

                        // the following query intermittently results in a
                        // NullPointerException
                        stat2.execute("CREATE VIEW INVOICE_DETAIL_VIEW" + j
                                + " as SELECT DTL.* FROM INVOICE_VIEW" + j
                                + " INV JOIN INVOICE_DETAIL_VIEW DTL ON INV.INVOICE_ID = DTL.INVOICE_ID"
                                + " WHERE DESCRIPTION='TEST'");

                        ResultSet rs = stat2
                                .executeQuery("SELECT * FROM INVOICE_VIEW" + j);
                        rs.next();
                        rs.close();

                        rs = stat2.executeQuery(
                                "SELECT * FROM INVOICE_DETAIL_VIEW" + j);
                        rs.next();
                        rs.close();

                        stat.close();
                        conn.close();

                    } catch (Exception ex) {
                        System.out.println("exception at iteration " + j + ":\n"
                                + getStackTrace(ex) + "\n");
                    }
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(24, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Utility method to get a stacktrace from a string.
     */
    public static String getStackTrace(Throwable t) {
        StringWriter stringWriter = new StringWriter();
        t.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

}
