/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.Trigger;

public class TriggerSample {

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE INVOICE(ID INT PRIMARY KEY, AMOUNT DECIMAL)");
        stat.execute("CREATE TABLE INVOICE_SUM(AMOUNT DECIMAL)");
        stat.execute("INSERT INTO INVOICE_SUM VALUES(0.0)");
        stat.execute("CREATE TRIGGER INV_INS AFTER INSERT ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
        stat.execute("CREATE TRIGGER INV_UPD AFTER UPDATE ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
        stat.execute("CREATE TRIGGER INV_DEL AFTER DELETE ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");

        stat.execute("INSERT INTO INVOICE VALUES(1, 10.0)");
        stat.execute("INSERT INTO INVOICE VALUES(2, 19.95)");
        stat.execute("UPDATE INVOICE SET AMOUNT=20.0 WHERE ID=2");
        stat.execute("DELETE FROM INVOICE WHERE ID=1");

        ResultSet rs;
        rs = stat.executeQuery("SELECT AMOUNT FROM INVOICE_SUM");
        rs.next();
        System.out.println("The sum is " + rs.getBigDecimal(1));
        conn.close();
    }

    public static class MyTrigger implements Trigger {

        public void init(Connection conn, String schemaName, String triggerName, String tableName) {
            // System.out.println("Initializing trigger " + triggerName + " for table " + tableName);
        }

        public void fire(Connection conn,
                Object[] oldRow, Object[] newRow)
                throws SQLException {
            BigDecimal diff = null;
            if(newRow != null) {
                diff = (BigDecimal)newRow[1];
            }
            if(oldRow != null) {
                BigDecimal m = (BigDecimal)oldRow[1];
                diff = diff == null ? m.negate() : diff.subtract(m);
            }
            PreparedStatement prep = conn.prepareStatement(
                    "UPDATE INVOICE_SUM SET AMOUNT=AMOUNT+?");
            prep.setBigDecimal(1, diff);
            prep.execute();
        }
    }

}
