package org.h2.test.cases;

/*
 * DatDb.java
 *
 * Created on 21 maggio 2007, 12.30
 *
 */

//package com.impl.util;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.sql.*;
import java.util.HashMap;
import java.util.Random;
/**
 *
 * @author fbi
 */
public class DatDb {


   private static DatDb datdb;


   static HashMap cP = new HashMap();
   static Connection sharedCon ;

   //private String url = "jdbc:h2:file:" + Constants.DAT_DB;
//   private String url = "jdbc:h2:file:/temp/datdb;TRACE_LEVEL_FILE=3";
   private String url = "jdbc:h2:file:/temp/datdb;DATABASE_EVENT_LISTENER='org.h2.samples.ShowProgress'";

   /** Creates a new instance of DatDb */
   protected DatDb() {
       try {
           long t1 = System.currentTimeMillis();
           Class.forName("org.h2.Driver");
           System.out.println (" getConnection --> ");
           //This is a 'special' connection used only to execute select statements (no transactions)
           sharedCon =  DriverManager.getConnection(url, "sa", "");
           System.out.println (" getConnection <-- " + (System.currentTimeMillis()- t1));
           sharedCon.setReadOnly(true);

       } catch (Exception e) {
           //Util.logThrowable(e);
           System.out.println(" error: " + e.getMessage());
       }
   }

   public static synchronized DatDb getInstance() {
       if (datdb == null) {
           datdb = new DatDb();
       }
       return datdb;
   }

   public void init (String tableName) {
       //String sc = ma.substring(ma.lastIndexOf("/")+1);
       try {
           Connection con =  DriverManager.getConnection(url, "sa", "");
           cP.put(tableName, con);
           createTable(con, tableName);
       } catch (Exception e) {
           //Util.logThrowable(e);
       }
   }



   public void save(String tableName, String key, String xml) {
       Connection con = null;
       //String tableName = ma.substring(ma.lastIndexOf("/")+1);
       try {
           con = (Connection) cP.get(tableName);
           if (con == null) throw new RuntimeException ("Unable to find the connection");

           con.setAutoCommit(false);
           PreparedStatement stm = null;

           stm = con.prepareStatement("delete from "+tableName+" where fname = ?");

           stm.setString(1, key);
           stm.executeUpdate();
           stm.close();
           stm = con.prepareStatement("insert into "+tableName+" (fname, gendate, dat) values(?,?,?)");
           stm.setString(1, key);
           stm.setLong(2, System.currentTimeMillis());
           //--- original code ---->>>>   stm.setObject(3, com.fw.util.Util.compressString(xml));
           stm.setObject(3, xml);
           stm.executeUpdate();
           con.commit();
       } catch (Exception e) {
           try {
               con.rollback();
           } catch (SQLException ex) {
               ex.printStackTrace();
           }
         //  Util.logThrowable(e);
       }
   }
   public long getGenDate (String tableName, String key) {
       Connection con = null;
       long gendate = 0;
       PreparedStatement stm = null;
       try {
           stm = sharedCon.prepareStatement("select gendate from "+tableName+" where fname = ?");
           stm.setString(1, key);
           ResultSet rs = stm.executeQuery();
           while (rs.next()) {
               gendate = rs.getLong(1);
           }
       } catch (Exception e) {
           //Util.logThrowable(e);
       } finally {
           try {
               stm.close();
           } catch (SQLException ex) {
               ex.printStackTrace();
           }

       }
       //System.out.println ("getGenDate table "+tableName+" key "+key+" returns "+gendate);
       return gendate;

   }
   public boolean containsKey(String tableName, String key) {
       Connection con = null;
       boolean b = false;
       PreparedStatement stm = null;
       try {
           stm = sharedCon.prepareStatement("select fname from "+tableName+" where fname = ?");
           stm.setString(1, key);
           ResultSet rs = stm.executeQuery();
           while (rs.next()) {
               b = true;
           }

       } catch (Exception e) {
           //Util.logThrowable(e);
       } finally {
           try {
               stm.close();
           } catch (SQLException ex) {
               ex.printStackTrace();
           }
       }
       //System.out.println ("containsKey table "+tableName+" key "+key+" returns "+b);
       return b;
   }

   public String getDat(String tableName, String key) {

       String ret = null;
       byte buf[] = null;
       PreparedStatement stm = null;
       try {
           stm = sharedCon.prepareStatement("select dat from "+tableName+" where fname = ?");
           stm.setString(1, key);
           ResultSet rs = stm.executeQuery();

           while (rs.next()) {
               buf = rs.getBytes(1);
           }
           if (buf == null) {
               //System.out.println (" NULL BUF for table "+tableName+" KEY "+ key);
               return ret;
           }
           // ------->>>> Original code ------<<<<  ret = Util.uncompressString(buf);
           ret = new String(buf);
       } catch (Exception ex) {
           ex.printStackTrace();
       } finally {
           try {
               stm.close();
           } catch (SQLException ex) {
               ex.printStackTrace();
           }
       }
       return ret;
   }

   private void createTable(Connection con, String tableName) {
       ResultSet rs = null;
       try {
           DatabaseMetaData dmd = con.getMetaData();
           rs = dmd.getTables(null, null, null, null);
           String temp = null;
           while (rs.next()) {
               temp = rs.getString(3);
               if (temp.equalsIgnoreCase(tableName)) {
                   return;
               }
           }
       } catch (Exception e) {
           //Util.logThrowable(e);
       }
       try {
           java.sql.Statement stm = con.createStatement();
           stm.execute("CREATE  TABLE "+tableName+" (fname varchar(80) primary key, gendate bigint, dat varchar) ");
 System.out.println (" Table "+tableName+" created");
       } catch (Exception e) {
//            Util.logThrowable(e);
       }
   }

   public void close(String tableName) {
       Connection c = (Connection) cP.remove(tableName);
       try {
           c.close();
       } catch (SQLException ex) {
           ex.printStackTrace();
       }
   }

//-------------------------------------- TEST START ------------------------

   public static void main(String ar[]) {
       System.out.println (" Starting ");
       DatDb db = new DatDb();
       db.go();
   }
   private void go () {
/*
 * Please change the values as needed.
 * The test I've made uses MAX_REC = 400000
 * and MAX_TAB = 50,
 * 20.000.000 records!
 */
       int MAX_REC = 400000;
       int MAX_TAB = 10;
       String tableName = null;
       long start = System.currentTimeMillis();

       System.out.println(" Start test ");


       long trec = 0;
       for (int j = 0; j < MAX_TAB; j++) {

           tableName = "n"+j;
           try {
               Connection con =  DriverManager.getConnection(url, "sa","");
               cP.put(tableName, con);
               createTable(con, tableName);

           } catch (Exception e ){ e.printStackTrace(); }

           long t1 = System.currentTimeMillis();
           trec = t1;

           for (int k = 0; k < MAX_REC; k++ ){

               this.save(tableName,"this_is_a_possible_key_value"+k, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm");
               if (k > 0 && (k % 10000) == 0) {
                   System.out.println(" added more 10000 records to "+tableName+" spent  "+ (System.currentTimeMillis()-trec)+ " K "+k);
                   trec = System.currentTimeMillis();
               }
           }

           close(tableName);

           System.out.println(" TABLE "+tableName+" completed with "+MAX_REC +" millis "+ (System.currentTimeMillis() - t1));

       }

       // Now, try to get back some raws
       int tkey = 0;
       int rkey = 0;
       Random rnd = new Random();
       for (int l = 0; l < 100; l++) {
           start = System.currentTimeMillis();
           tkey = rnd.nextInt(MAX_TAB);
           rkey = rnd.nextInt(MAX_REC);
           this.getDat("n"+tkey, "questa_essere_la_chivae_numero_"+rkey);
//           System.out.println ("estract dat key "+rkey+" from table n"+ tkey+" in millis "+ (System.currentTimeMillis() - start));
       }

       System.out.println (" exercise end" );
       try {
           sharedCon.close();
       } catch (SQLException ex) {
           ex.printStackTrace();
       }
   }


}
