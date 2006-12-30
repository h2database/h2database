/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeSet;

import org.h2.test.TestBase;

public class TestOptimizations extends TestBase {

    public void test() throws Exception {
        if(config.networked) {
            return;
        }
        testQueryCacheTimestamp();
        testQueryCacheSpeed();
        testQueryCache(true);
        testQueryCache(false);
        testIn();
        testMinMaxCountOptimization(true);
        testMinMaxCountOptimization(false);
    }
    
    private void testQueryCacheTimestamp() throws Exception {
        deleteDb("optimizations");
        Connection conn=getConnection("optimizations");
        PreparedStatement prep = conn.prepareStatement("SELECT CURRENT_TIMESTAMP()");
        ResultSet rs = prep.executeQuery();
        rs.next();
        String a = rs.getString(1);
        Thread.sleep(50);
        rs = prep.executeQuery();
        rs.next();
        String b = rs.getString(1);
        checkFalse(a.equals(b));
        conn.close();        
    }
    
    private void testQueryCacheSpeed() throws Exception {
        deleteDb("optimizations");
        Connection conn=getConnection("optimizations");
        Statement stat = conn.createStatement();
        testQuerySpeed(stat, "select sum(x) from system_range(1, 10000) a where a.x in (select b.x from system_range(1, 30) b)");
        testQuerySpeed(stat, "select sum(a.n), sum(b.x) from system_range(1, 100) b, (select sum(x) n from system_range(1, 4000)) a");
        conn.close();
    }
    
    private void testQuerySpeed(Statement stat, String sql) throws Exception {
        stat.execute("set OPTIMIZE_REUSE_RESULTS 0");
        stat.execute(sql);
        long time = System.currentTimeMillis();
        stat.execute(sql);
        time = System.currentTimeMillis() - time;
        stat.execute("set OPTIMIZE_REUSE_RESULTS 1");
        stat.execute(sql);
        long time2 = System.currentTimeMillis();
        stat.execute(sql);
        time2 = System.currentTimeMillis() - time2;
        if(time2 > time) {
            error("not optimized: " + time + " optimized: " + time2+ " sql:"+sql);
        }
    }
    
    private void testQueryCache(boolean optimize) throws Exception {
        deleteDb("optimizations");
        Connection conn=getConnection("optimizations");
        Statement stat = conn.createStatement();
        if(optimize) {
            stat.execute("set OPTIMIZE_REUSE_RESULTS 1");
        } else {
            stat.execute("set OPTIMIZE_REUSE_RESULTS 0");
        }
        stat.execute("create table test(id int)");
        stat.execute("create table test2(id int)");
        stat.execute("insert into test values(1), (1), (2)");
        stat.execute("insert into test2 values(1)");
        PreparedStatement prep = conn.prepareStatement("select * from test where id = (select id from test2)");
        ResultSet rs1 = prep.executeQuery();
        rs1.next();
        check(rs1.getInt(1), 1);
        rs1.next();
        check(rs1.getInt(1), 1);
        checkFalse(rs1.next());
        
        stat.execute("update test2 set id = 2");
        ResultSet rs2 = prep.executeQuery();
        rs2.next();
        check(rs2.getInt(1), 2);
        
        conn.close();
    }

    private void testMinMaxCountOptimization(boolean memory) throws Exception {
        deleteDb("optimizations");
        Connection conn=getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create "+(memory?"memory":"") +" table test(id int primary key, value int)");
        stat.execute("create index idxvalueid on test(value, id);");
        int len = getSize(1000, 10000);
        HashMap map = new HashMap();
        TreeSet set = new TreeSet();
        Random random = new Random(1);
        for(int i=0; i<len ; i++) {
            if(i==len / 2) {
                if(!config.memory) {
                    conn.close();
                    conn=getConnection("optimizations");
                    stat = conn.createStatement();
                }
            }
            switch(random.nextInt(10)) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
                if(random.nextInt(1000)==1) {
                    stat.execute("insert into test values("+i+", null)");
                    map.put(new Integer(i), null);
                } else {
                    int value = random.nextInt();
                    stat.execute("insert into test values("+i+", "+value+")");
                    map.put(new Integer(i), new Integer(value));
                    set.add(new Integer(value));
                }
                break;
            case 6:
            case 7:
            case 8: {
                if(map.size()>0) {
                    for(int j=random.nextInt(i), k=0; k<10; k++, j++) {
                        if(map.containsKey(new Integer(j))) {
                            Integer x = (Integer) map.remove(new Integer(j));
                            if(x != null) {
                                set.remove(x);
                            }
                            stat.execute("delete from test where id="+j);
                        }
                    }
                }
                break;
            }
            case 9: {
                ArrayList list = new ArrayList(map.values());
                int count = list.size();
                Integer min = null, max = null;
                if(count > 0) {
                    min = (Integer) set.first();
                    max = (Integer) set.last();
                }
                ResultSet rs = stat.executeQuery("select min(value), max(value), count(*) from test");
                rs.next();
                Integer minDb = (Integer)rs.getObject(1);
                Integer maxDb = (Integer)rs.getObject(2);
                int countDb = rs.getInt(3);
                check(minDb, min);
                check(maxDb, max);
                check(countDb, count);
            }
            }
        }
        conn.close();
    }

    private void testIn() throws Exception {
        deleteDb("optimizations");
        Connection conn=getConnection("optimizations");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("insert into test values(2, 'World')");
        PreparedStatement prep;
        ResultSet rs;
        
        prep = conn.prepareStatement("select * from test t1 where t1.id in(?)");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id in(?, ?) order by id");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getString(2), "World");
        checkFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id in(select t2.id from test t2 where t2.id=?)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getString(2), "World");
        checkFalse(rs.next());

        prep = conn.prepareStatement("select * from test t1 where t1.id in(select t2.id from test t2 where t2.id=? and t1.id<>t2.id)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        checkFalse(rs.next());
        
        prep = conn.prepareStatement("select * from test t1 where t1.id in(select t2.id from test t2 where t2.id in(cast(?+10 as varchar)))");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        checkFalse(rs.next());        

        conn.close();
    }
    

}
