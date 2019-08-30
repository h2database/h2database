/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;

/**
 * A very simple class that shows how to load the driver, create a database,
 * create a table, and insert some data.
 */
public class CatalogTest {

    /**
     * Called when ran from command line.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        // delete the database named 'test' in the user home directory
        DeleteDbFiles.execute("~", "test", true);

        Class.forName("org.h2.Driver");
        try (Connection conn = DriverManager.getConnection("jdbc:h2:~/test;IGNORE_CATALOGS=TRUE")) {
            try (Statement stat = conn.createStatement()) {

                stat.execute("create schema dbo");
                stat.execute("set schema dbo");
                stat.execute("create table dbo.test(id int primary key, name varchar(255))");
                stat.execute("create table catalog1.dbo.test2(id int primary key, name varchar(255))");
                stat.execute("insert into dbo.test values(1, 'Hello')");
                stat.execute("insert into dbo.test2 values(1, 'Hello')");
                stat.execute("set ignore_catalogs=false");
                try {
                    stat.execute("insert into catalog1.dbo.test2 values(2, 'Hello2')");
                    throw new RuntimeException("expected exception because of not existing catalog name.");

                } catch (Exception e) {
                    stat.execute("set ignore_catalogs=true");
                }
                stat.execute("insert into catalog1.dbo.test2 values(2, 'Hello2')");
                try (ResultSet rs = stat.executeQuery("select * from test")) {
                    while (rs.next()) {
                        System.out.println(rs.getString("name"));
                    }
                }
                try (ResultSet rs = stat.executeQuery("select * from catalogx.dbo.test2")) {
                    while (rs.next()) {
                        System.out.println(rs.getString("name"));
                    }
                }
            }
        }
    }

}
