package org.h2.my.test;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * MyH2Server
 *
 * @author longhuashen
 * @since 2021-11-16
 */
public class MyH2Server {

    public static void main(String[] args) throws SQLException {
        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("h2.lobInDatabase", "false");
        System.setProperty("h2.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", "./target/mytest/tmp");
        System.setProperty("h2.baseDir", "./target/mytest");
        // System.setProperty("h2.check2", "true");
        ArrayList<String> list = new ArrayList<String>();
        // list.add("-tcp");
        // //list.add("-tool");
        // org.h2.tools.Server.main(list.toArray(new String[list.size()]));

        //list.add("-help");
        //list.add("-webXXXX"); //测试showUsageAndThrowUnsupportedOption

        //
        // list.add("-tcp");
        // list.add("-tcpPort");
        // list.add("9092");

        // 测试org.h2.server.TcpServer.checkKeyAndGetDatabaseName(String)
        // list.add("-key");
        // list.add("mydb");
        // list.add("mydatabase");


        // list.add("-browser");

        // list.add("-pg");
        list.add("-tcp");
        // list.add("-web");
        // list.add("-ifExists");
        list.add("-ifNotExists");
        list.add("-tcpAllowOthers");
        list.add("-tcpPassword");
        list.add("aaa");
        list.add("-webAdminPassword");
        list.add("aaa");
        org.h2.tools.Server.main(list.toArray(new String[list.size()]));
    }
}
