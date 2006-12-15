/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.sql.*;
import java.math.*;

public class BenchC implements Bench {

    private Database db;

    int warehouses = 1;
    int items = 10000;
    int districtsPerWarehouse = 10;
    int customersPerDistrict = 300;
    private int ordersPerDistrict = 300;

    private BenchCRandom random;
    private String action;
    
    private int commitEvery = 1000;    

//    private final static String[] OPERATION_TEXT = { "Neworder", "Payment",
//            "Order Status", "Delivery (deferred)", "Delivery (interactive)",
//            "Stock-level" };
//
    private static final String[] TABLES = new String[] { "WAREHOUSE",
            "DISTRICT", "CUSTOMER", "HISTORY", "ORDERS", "NEW_ORDER", "ITEM",
            "STOCK", "ORDER_LINE", "RESULTS" };
    private static final String[] CREATE_SQL = new String[] {
            "CREATE TABLE  WAREHOUSE(\n" + " W_ID INT NOT NULL PRIMARY KEY,\n"
                    + " W_NAME VARCHAR(10),\n" + " W_STREET_1 VARCHAR(20),\n"
                    + " W_STREET_2 VARCHAR(20),\n" + " W_CITY VARCHAR(20),\n"
                    + " W_STATE CHAR(2),\n" + " W_ZIP CHAR(9),\n"
                    + " W_TAX DECIMAL(4, 4),\n" + " W_YTD DECIMAL(12, 2))",
            "CREATE TABLE  DISTRICT(\n" + " D_ID INT NOT NULL,\n" + " D_W_ID INT NOT NULL,\n"
                    + " D_NAME VARCHAR(10),\n" + " D_STREET_1 VARCHAR(20),\n"
                    + " D_STREET_2 VARCHAR(20),\n" + " D_CITY VARCHAR(20),\n"
                    + " D_STATE CHAR(2),\n" + " D_ZIP CHAR(9),\n"
                    + " D_TAX DECIMAL(4, 4),\n" + " D_YTD DECIMAL(12, 2),\n"
                    + " D_NEXT_O_ID INT,\n"
                    + " PRIMARY KEY (D_ID, D_W_ID))", //,\n"
//                    + " FOREIGN KEY (D_W_ID)\n"
//                    + "  REFERENCES WAREHOUSE(W_ID))",
            "CREATE TABLE  CUSTOMER(\n" + " C_ID INT NOT NULL,\n" + " C_D_ID INT NOT NULL,\n"
                    + " C_W_ID INT NOT NULL,\n" + " C_FIRST VARCHAR(16),\n"
                    + " C_MIDDLE CHAR(2),\n" + " C_LAST VARCHAR(16),\n"
                    + " C_STREET_1 VARCHAR(20),\n"
                    + " C_STREET_2 VARCHAR(20),\n" + " C_CITY VARCHAR(20),\n"
                    + " C_STATE CHAR(2),\n" + " C_ZIP CHAR(9),\n"
                    + " C_PHONE CHAR(16),\n" + " C_SINCE TIMESTAMP,\n"
                    + " C_CREDIT CHAR(2),\n"
                    + " C_CREDIT_LIM DECIMAL(12, 2),\n"
                    + " C_DISCOUNT DECIMAL(4, 4),\n"
                    + " C_BALANCE DECIMAL(12, 2),\n"
                    + " C_YTD_PAYMENT DECIMAL(12, 2),\n"
                    + " C_PAYMENT_CNT DECIMAL(4),\n"
                    + " C_DELIVERY_CNT DECIMAL(4),\n"
                    + " C_DATA VARCHAR(500),\n"
                    + " PRIMARY KEY (C_W_ID, C_D_ID, C_ID))", //,\n"
//                    + " FOREIGN KEY (C_W_ID, C_D_ID)\n"
//                    + "  REFERENCES DISTRICT(D_W_ID, D_ID))",
            "CREATE INDEX CUSTOMER_NAME ON CUSTOMER(C_LAST, C_D_ID, C_W_ID)",
            "CREATE TABLE  HISTORY(\n" + " H_C_ID INT,\n" + " H_C_D_ID INT,\n"
                    + " H_C_W_ID INT,\n" + " H_D_ID INT,\n" + " H_W_ID INT,\n"
                    + " H_DATE TIMESTAMP,\n" + " H_AMOUNT DECIMAL(6, 2),\n"
                    + " H_DATA VARCHAR(24))", //,\n"
//                    + " FOREIGN KEY(H_C_W_ID, H_C_D_ID, H_C_ID)\n"
//                    + "  REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID),\n"
//                    + " FOREIGN KEY(H_W_ID, H_D_ID)\n"
//                    + "  REFERENCES DISTRICT(D_W_ID, D_ID))",
            "CREATE TABLE  ORDERS(\n" + " O_ID INT NOT NULL,\n" + " O_D_ID INT NOT NULL,\n"
                    + " O_W_ID INT NOT NULL,\n" + " O_C_ID INT,\n"
                    + " O_ENTRY_D TIMESTAMP,\n" + " O_CARRIER_ID INT,\n"
                    + " O_OL_CNT INT,\n" + " O_ALL_LOCAL DECIMAL(1),\n"
                    + " PRIMARY KEY(O_W_ID, O_D_ID, O_ID))", // ,\n"
//                    + " FOREIGN KEY(O_W_ID, O_D_ID, O_C_ID)\n"
//                    + "  REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID))",
            "CREATE INDEX ORDERS_OID ON ORDERS(O_ID)",
            "CREATE TABLE  NEW_ORDER(\n" + " NO_O_ID INT NOT NULL,\n" + " NO_D_ID INT NOT NULL,\n"
                    + " NO_W_ID INT NOT NULL,\n"
                    + " PRIMARY KEY(NO_W_ID, NO_D_ID, NO_O_ID))", //,\n"
//                    + " FOREIGN KEY(NO_W_ID, NO_D_ID, NO_O_ID)\n"
//                    + " REFERENCES ORDER(O_W_ID, O_D_ID, O_ID))",
            "CREATE TABLE  ITEM(\n" + " I_ID INT NOT NULL,\n" + " I_IM_ID INT,\n"
                    + " I_NAME VARCHAR(24),\n" + " I_PRICE DECIMAL(5, 2),\n"
                    + " I_DATA VARCHAR(50),\n" + " PRIMARY KEY(I_ID))",
            "CREATE TABLE  STOCK(\n" + " S_I_ID INT NOT NULL,\n" + " S_W_ID INT NOT NULL,\n"
                    + " S_QUANTITY DECIMAL(4),\n" + " S_DIST_01 CHAR(24),\n"
                    + " S_DIST_02 CHAR(24),\n" + " S_DIST_03 CHAR(24),\n"
                    + " S_DIST_04 CHAR(24),\n" + " S_DIST_05 CHAR(24),\n"
                    + " S_DIST_06 CHAR(24),\n" + " S_DIST_07 CHAR(24),\n"
                    + " S_DIST_08 CHAR(24),\n" + " S_DIST_09 CHAR(24),\n"
                    + " S_DIST_10 CHAR(24),\n" + " S_YTD DECIMAL(8),\n"
                    + " S_ORDER_CNT DECIMAL(4),\n"
                    + " S_REMOTE_CNT DECIMAL(4),\n" + " S_DATA VARCHAR(50),\n"
                    + " PRIMARY KEY(S_W_ID, S_I_ID))", //,\n"
//                    + " FOREIGN KEY(S_W_ID)\n"
//                    + " REFERENCES WAREHOUSE(W_ID),\n"
//                    + " FOREIGN KEY(S_I_ID)\n" + "  REFERENCES ITEM(I_ID))",
            "CREATE TABLE  ORDER_LINE(\n"
                    + " OL_O_ID INT NOT NULL,\n"
                    + " OL_D_ID INT NOT NULL,\n"
                    + " OL_W_ID INT NOT NULL,\n"
                    + " OL_NUMBER INT NOT NULL,\n"
                    + " OL_I_ID INT,\n"
                    + " OL_SUPPLY_W_ID INT,\n"
                    + " OL_DELIVERY_D TIMESTAMP,\n"
                    + " OL_QUANTITY DECIMAL(2),\n"
                    + " OL_AMOUNT DECIMAL(6, 2),\n"
                    + " OL_DIST_INFO CHAR(24),\n"
                    + " PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER))", //,\n"
//                    + " FOREIGN KEY(OL_W_ID, OL_D_ID, OL_O_ID)\n"
//                    + "  REFERENCES ORDER(O_W_ID, O_D_ID, O_ID),\n"
//                    + " FOREIGN KEY(OL_SUPPLY_W_ID, OL_I_ID)\n"
//                    + "  REFERENCES STOCK(S_W_ID, S_I_ID))",
            "CREATE TABLE RESULTS(\n" + " ID INT NOT NULL PRIMARY KEY,\n"
                    + " TERMINAL INT,\n" + " OPERATION INT,\n"
                    + " RESPONSE_TIME INT,\n" + " PROCESSING_TIME INT,\n"
                    + " KEYING_TIME INT,\n" + " THINK_TIME INT,\n"
                    + " SUCCESSFULL INT,\n" + " NOW TIMESTAMP)" };

    public void init(Database db, int size) throws Exception {
        this.db = db;
        
        random = new BenchCRandom();
        
        items = size * 10;
        warehouses = 1;
        districtsPerWarehouse = Math.max(1, size / 100);
        customersPerDistrict = Math.max(1, size / 100);
        ordersPerDistrict = Math.max(1, size / 1000);
        
        db.start(this, "Init");
        db.openConnection();
        load();
        db.commit();
        db.closeConnection();
        db.end();
        
        db.start(this, "Open/Close");
        db.openConnection();
        db.closeConnection();
        db.end();
        
    }
    
    private void load() throws Exception {
        for (int i = 0; i < TABLES.length; i++) {
            db.dropTable(TABLES[i]);
        }
        for (int i = 0; i < CREATE_SQL.length; i++) {
            db.update(CREATE_SQL[i]);
        }
        db.setAutoCommit(false);
        loadItem();
        loadWarehouse();
        loadCustomer();
        loadOrder();
        db.commit();
        trace("load done");
    }

    

    void trace(String s) {
        action = s;
    }

    void trace(int i,int max) {
        db.trace(action, i, max);
    }

    private void loadItem() throws Exception {
        trace("load item");
        boolean[] original = random.getBoolean(items, items / 10);
        PreparedStatement prep = db.prepare("INSERT INTO ITEM(I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA) "
                        + "VALUES(?, ?, ?, ?, ?)");
        for (int i_id = 1; i_id <= items; i_id++) {
            String i_name = random.getString(14, 24);
            BigDecimal i_price = random.getBigDecimal(random.getInt(100, 10000), 2);
            String i_data = random.getString(26, 50);
            if (original[i_id - 1]) {
                i_data = random.replace(i_data, "original");
            }
            prep.setInt(1, i_id);
            prep.setInt(2, random.getInt(1, 10000));
            prep.setString(3, i_name);
            prep.setBigDecimal(4, i_price);
            prep.setString(5, i_data);
            db.update(prep);
            trace(i_id, items);
            if(i_id%commitEvery==0) {
                db.commit();
            }              
        }
    }

    private void loadWarehouse() throws Exception {
        trace("loading warehouses");
        PreparedStatement prep = db.prepare("INSERT INTO WAREHOUSE(W_ID, W_NAME, W_STREET_1, "
                        + "W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) "
                        + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
        for (int w_id = 1; w_id <= warehouses; w_id++) {
            String w_name = random.getString(6, 10);
            String[] address = random.getAddress();
            String w_street_1 = address[0];
            String w_street_2 = address[1];
            String w_city = address[2];
            String w_state = address[3];
            String w_zip = address[4];
            BigDecimal w_tax = random.getBigDecimal(random.getInt(0, 2000), 4);
            BigDecimal w_ytd = new BigDecimal("300000.00");
            prep.setInt(1, w_id);
            prep.setString(2, w_name);
            prep.setString(3, w_street_1);
            prep.setString(4, w_street_2);
            prep.setString(5, w_city);
            prep.setString(6, w_state);
            prep.setString(7, w_zip);
            prep.setBigDecimal(8, w_tax);
            prep.setBigDecimal(9, w_ytd);
            db.update(prep);
            loadStock(w_id);
            loadDistrict(w_id);
            if(w_id%commitEvery==0) {
                db.commit();
            }     
        }
    }

    private void loadCustomer() throws Exception {
        trace("load customers");
        int max = warehouses * districtsPerWarehouse;
        int i = 0;
        for (int w_id = 1; w_id <= warehouses; w_id++) {
            for (int d_id = 1; d_id <= districtsPerWarehouse; d_id++) {
                loadCustomerSub(d_id, w_id);
                trace(i++, max);
                if(i%commitEvery==0) {
                    db.commit();
                }                     
            }
        }
    }

    private void loadCustomerSub(int d_id, int w_id) throws Exception {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        PreparedStatement prepCustomer = db.prepare("INSERT INTO CUSTOMER(C_ID, C_D_ID, C_W_ID, "
                        + "C_FIRST, C_MIDDLE, C_LAST, "
                        + "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, "
                        + "C_PHONE, C_SINCE, C_CREDIT, "
                        + "C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA, "
                        + "C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement prepHistory = db.prepare("INSERT INTO HISTORY(H_C_ID, H_C_D_ID, H_C_W_ID, "
                        + "H_W_ID, H_D_ID, H_DATE, H_AMOUNT, H_DATA) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        for (int c_id = 1; c_id <= customersPerDistrict; c_id++) {
            int c_d_id = d_id;
            int c_w_id = w_id;
            String c_first = random.getString(8, 16);
            String c_middle = "OE";
            String c_last;
            if (c_id < 1000) {
                c_last = random.getLastname(c_id);
            } else {
                c_last = random.getLastname(random.getNonUniform(255, 0, 999));
            }
            String[] address = random.getAddress();
            String c_street_1 = address[0];
            String c_street_2 = address[1];
            String c_city = address[2];
            String c_state = address[3];
            String c_zip = address[4];
            String c_phone = random.getNumberString(16, 16);
            String c_credit;
            if (random.getInt(0, 1) == 0) {
                c_credit = "GC";
            } else {
                c_credit = "BC";
            }
            BigDecimal c_discount = random.getBigDecimal(random.getInt(0, 5000), 4);
            BigDecimal c_balance = new BigDecimal("-10.00");
            BigDecimal c_credit_lim = new BigDecimal("50000.00");
            String c_data = random.getString(300, 500);
            BigDecimal c_ytd_payment = new BigDecimal("10.00");
            int c_payment_cnt = 1;
            int c_delivery_cnt = 1;
            prepCustomer.setInt(1, c_id);
            prepCustomer.setInt(2, c_d_id);
            prepCustomer.setInt(3, c_w_id);
            prepCustomer.setString(4, c_first);
            prepCustomer.setString(5, c_middle);
            prepCustomer.setString(6, c_last);
            prepCustomer.setString(7, c_street_1);
            prepCustomer.setString(8, c_street_2);
            prepCustomer.setString(9, c_city);
            prepCustomer.setString(10, c_state);
            prepCustomer.setString(11, c_zip);
            prepCustomer.setString(12, c_phone);
            prepCustomer.setTimestamp(13, timestamp);
            prepCustomer.setString(14, c_credit);
            prepCustomer.setBigDecimal(15, c_credit_lim);
            prepCustomer.setBigDecimal(16, c_discount);
            prepCustomer.setBigDecimal(17, c_balance);
            prepCustomer.setString(18, c_data);
            prepCustomer.setBigDecimal(19, c_ytd_payment);
            prepCustomer.setInt(20, c_payment_cnt);
            prepCustomer.setInt(21, c_delivery_cnt);
            db.update(prepCustomer);            
            BigDecimal h_amount = new BigDecimal("10.00");
            String h_data = random.getString(12, 24);
            prepHistory.setInt(1, c_id);
            prepHistory.setInt(2, c_d_id);
            prepHistory.setInt(3, c_w_id);
            prepHistory.setInt(4, c_w_id);
            prepHistory.setInt(5, c_d_id);
            prepHistory.setTimestamp(6, timestamp);
            prepHistory.setBigDecimal(7, h_amount);
            prepHistory.setString(8, h_data);
            db.update(prepHistory);
        }
    }

    private void loadOrder() throws Exception {
        trace("load orders");
        int max = warehouses * districtsPerWarehouse;
        int i = 0;
        for (int w_id = 1; w_id <= warehouses; w_id++) {
            for (int d_id = 1; d_id <= districtsPerWarehouse; d_id++) {
                loadOrderSub(d_id, w_id);
                trace(i++, max);
            }
        }
    }

    private void loadOrderSub(int d_id, int w_id) throws Exception {
        int o_d_id = d_id;
        int o_w_id = w_id;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        int[] orderid = random.getPermutation(ordersPerDistrict);
        PreparedStatement prepOrder = db.prepare("INSERT INTO ORDERS(O_ID, O_C_ID, O_D_ID, O_W_ID, "
                        + "O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) "
                        + "VALUES(?, ?, ?, ?, ?, ?, ?, 1)");
        PreparedStatement prepNewOrder = db.prepare("INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) "
                        + "VALUES (?, ?, ?)");
        PreparedStatement prepLine = db.prepare("INSERT INTO ORDER_LINE("
                        + "OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, "
                        + "OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, "
                        + "OL_DIST_INFO, OL_DELIVERY_D)"
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)");
        for (int o_id = 1, i=0; o_id <= ordersPerDistrict; o_id++) {
            int o_c_id = orderid[o_id - 1];
            int o_carrier_id = random.getInt(1, 10);
            int o_ol_cnt = random.getInt(5, 15);
            prepOrder.setInt(1, o_id);
            prepOrder.setInt(2, o_c_id);
            prepOrder.setInt(3, o_d_id);
            prepOrder.setInt(4, o_w_id);
            prepOrder.setTimestamp(5, timestamp);
            prepOrder.setInt(7, o_ol_cnt);
            if (o_id <= 2100) {
                prepOrder.setInt(6, o_carrier_id);
            } else {
                // the last 900 orders have not been delivered
                prepOrder.setNull(6, Types.INTEGER);
                prepNewOrder.setInt(1, o_id);
                prepNewOrder.setInt(2, o_d_id);
                prepNewOrder.setInt(3, o_w_id);
                db.update(prepNewOrder);
            }
            db.update(prepOrder);
            for (int ol = 1; ol <= o_ol_cnt; ol++) {
                int ol_i_id = random.getInt(1, items);
                int ol_supply_w_id = o_w_id;
                int ol_quantity = 5;
                String ol_dist_info = random.getString(24);
                BigDecimal ol_amount;
                if (o_id < 2101) {
                    ol_amount = random.getBigDecimal(0, 2);
                } else {
                    ol_amount = random.getBigDecimal(random.getInt(0, 1000000), 2);
                }
                prepLine.setInt(1, o_id);
                prepLine.setInt(2, o_d_id);
                prepLine.setInt(3, o_w_id);
                prepLine.setInt(4, ol);
                prepLine.setInt(5, ol_i_id);
                prepLine.setInt(6, ol_supply_w_id);
                prepLine.setInt(7, ol_quantity);
                prepLine.setBigDecimal(8, ol_amount);
                prepLine.setString(9, ol_dist_info);
                db.update(prepLine);
                if(i++%commitEvery==0) {
                    db.commit();
                }
            }
        }
    }

    private void loadStock(int w_id) throws Exception {
        trace("load stock (warehouse " + w_id + ")");
        int s_w_id = w_id;
        boolean[] original = random.getBoolean(items, items / 10);
        PreparedStatement prep = db.prepare("INSERT INTO STOCK(S_I_ID, S_W_ID, S_QUANTITY, "
                        + "S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
                        + "S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, "
                        + "S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT) "
                        + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        for (int s_i_id = 1; s_i_id <= items; s_i_id++) {
            int s_quantity = random.getInt(10, 100);
            String s_dist_01 = random.getString(24);
            String s_dist_02 = random.getString(24);
            String s_dist_03 = random.getString(24);
            String s_dist_04 = random.getString(24);
            String s_dist_05 = random.getString(24);
            String s_dist_06 = random.getString(24);
            String s_dist_07 = random.getString(24);
            String s_dist_08 = random.getString(24);
            String s_dist_09 = random.getString(24);
            String s_dist_10 = random.getString(24);
            String s_data = random.getString(26, 50);
            if (original[s_i_id - 1]) {
                s_data = random.replace(s_data, "original");
            }
            prep.setInt(1, s_i_id);
            prep.setInt(2, s_w_id);
            prep.setInt(3, s_quantity);
            prep.setString(4, s_dist_01);
            prep.setString(5, s_dist_02);
            prep.setString(6, s_dist_03);
            prep.setString(7, s_dist_04);
            prep.setString(8, s_dist_05);
            prep.setString(9, s_dist_06);
            prep.setString(10, s_dist_07);
            prep.setString(11, s_dist_08);
            prep.setString(12, s_dist_09);
            prep.setString(13, s_dist_10);
            prep.setString(14, s_data);
            prep.setInt(15, 0);
            prep.setInt(16, 0);
            prep.setInt(17, 0);
            db.update(prep);
            if(s_i_id%commitEvery==0) {
                db.commit();
            }            
            trace(s_i_id, items);
        }
    }

    private void loadDistrict(int w_id) throws Exception {
        int d_w_id = w_id;
        BigDecimal d_ytd = new BigDecimal("300000.00");
        int d_next_o_id = 3001;
        PreparedStatement prep = db.prepare("INSERT INTO DISTRICT(D_ID, D_W_ID, D_NAME, "
                        + "D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, "
                        + "D_TAX, D_YTD, D_NEXT_O_ID) "
                        + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        for (int d_id = 1; d_id <= districtsPerWarehouse; d_id++) {
            String d_name = random.getString(6, 10);
            String[] address = random.getAddress();
            String d_street_1 = address[0];
            String d_street_2 = address[1];
            String d_city = address[2];
            String d_state = address[3];
            String d_zip = address[4];
            BigDecimal d_tax = random.getBigDecimal(random.getInt(0, 2000), 4);
            prep.setInt(1, d_id);
            prep.setInt(2, d_w_id);
            prep.setString(3, d_name);
            prep.setString(4, d_street_1);
            prep.setString(5, d_street_2);
            prep.setString(6, d_city);
            prep.setString(7, d_state);
            prep.setString(8, d_zip);
            prep.setBigDecimal(9, d_tax);
            prep.setBigDecimal(10, d_ytd);
            prep.setInt(11, d_next_o_id);
            db.update(prep);
            trace(d_id, districtsPerWarehouse);
        }
    }

    public void run() throws Exception {
        db.start(this, "Transactions");
        db.openConnection();
        for(int i=0; i<70; i++) {
            BenchCThread process = new BenchCThread(db, this, random, i);
            process.process();
        }
        db.closeConnection();
        db.end();

        db.openConnection();
        BenchCThread process = new BenchCThread(db, this, random, 0);
        process.process();
        db.logMemory(this, "Memory Usage");
        db.closeConnection();
  }

    public String getName() {
        return "BenchC";
    }

}

