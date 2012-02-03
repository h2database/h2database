/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.h2.api.Trigger;

/**
 * This sample application shows how to pass data to a trigger.
 */
public class TriggerPassData implements Trigger {

    private static final Map<String, TriggerPassData> TRIGGERS =
        new HashMap<String, TriggerPassData>();
    private String triggerData;

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("CREATE ALIAS TRIGGER_SET FOR \"" +
                TriggerPassData.class.getName() +
                ".setTriggerData\"");
        stat.execute("CREATE TRIGGER T1 " +
                "BEFORE INSERT ON TEST " +
                "FOR EACH ROW CALL \"" +
                TriggerPassData.class.getName() + "\"");
        stat.execute("CALL TRIGGER_SET('T1', 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("CALL TRIGGER_SET('T1', 'World')");
        stat.execute("INSERT INTO TEST VALUES(2)");
        conn.close();
    }

    public void init(Connection conn, String schemaName,
            String triggerName, String tableName, boolean before,
            int type) {
        TRIGGERS.put(triggerName, this);
    }

    public void fire(Connection conn, Object[] old, Object[] row) {
        System.out.println(triggerData + ": " + row[0]);
    }

    /**
     * Call this method to change a specific trigger.
     *
     * @param trigger the trigger name
     * @param data the data
     */
    public static void setTriggerData(String trigger, String data) {
        TRIGGERS.get(trigger).triggerData = data;
    }

}
