/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;

public class BenchCThread {

    private Database db;
    private int warehouseId;
    private int terminalId;
    private HashMap prepared = new HashMap();
    private static final int OP_NEW_ORDER = 0, OP_PAYMENT = 1,
            OP_ORDER_STATUS = 2, OP_DELIVERY = 3,
            OP_STOCK_LEVEL = 4;
    private static final BigDecimal ONE = new BigDecimal("1");
    private BenchCRandom random;
    private BenchC bench;

    BenchCThread(Database db, BenchC bench, BenchCRandom random, int terminal)
            throws Exception {
        this.db = db;
        this.bench = bench;
        this.terminalId = terminal;
        db.setAutoCommit(false);
        this.random = random;
        warehouseId = random.getInt(1, bench.warehouses);
    }

    void process() throws Exception {
        int[] deck = new int[] { OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER,
                OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER,
                OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER, OP_PAYMENT,
                OP_PAYMENT, OP_PAYMENT, OP_PAYMENT, OP_PAYMENT, OP_PAYMENT,
                OP_PAYMENT, OP_PAYMENT, OP_PAYMENT, OP_PAYMENT,
                OP_ORDER_STATUS, OP_DELIVERY, OP_STOCK_LEVEL };
        int len = deck.length;
        for (int i = 0; i < len; i++) {
            int temp = deck[i];
            int j = random.getInt(0, len);
            deck[i] = deck[j];
            deck[j] = temp;
        }
        for (int i = 0; i < len; i++) {
            int op = deck[i];
            switch (op) {
            case OP_NEW_ORDER:
                processNewOrder();
                break;
            case OP_PAYMENT:
                processPayment();
                break;
            case OP_ORDER_STATUS:
                processOrderStatus();
                break;
            case OP_DELIVERY:
                processDelivery();
                break;
            case OP_STOCK_LEVEL:
                processStockLevel();
                break;
            default:
                throw new Error("op=" + op);
            }
        }
    }

    private void processNewOrder() throws Exception {
        int d_id = random.getInt(1, bench.districtsPerWarehouse);
        int c_id = random.getNonUniform(1023, 1, bench.customersPerDistrict);
        int o_ol_cnt = random.getInt(5, 15);
        boolean rollback = random.getInt(1, 100) == 1;
        int[] supply_w_id = new int[o_ol_cnt];
        int[] item_id = new int[o_ol_cnt];
        int[] quantity = new int[o_ol_cnt];
        int o_all_local = 1;
        for (int i = 0; i < o_ol_cnt; i++) {
            int w;
            if (bench.warehouses > 1 && random.getInt(1, 100) == 1) {
                do {
                    w = random.getInt(1, bench.warehouses);
                } while (w != warehouseId);
                o_all_local = 0;
            } else {
                w = warehouseId;
            }
            supply_w_id[i] = w;
            int item;
            if (rollback && i == o_ol_cnt - 1) {
                // unused order number
                item = -1;
            } else {
                item = random.getNonUniform(8191, 1, bench.items);
            }
            item_id[i] = item;
            quantity[i] = random.getInt(1, 10);
        }
        char[] bg = new char[o_ol_cnt];
        int[] stock = new int[o_ol_cnt];
        BigDecimal[] amt = new BigDecimal[o_ol_cnt];
        Timestamp datetime = new Timestamp(System.currentTimeMillis());
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=D_NEXT_O_ID+1 "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, d_id);
        prep.setInt(2, warehouseId);
        db.update(prep, "updateDistrict");
        prep = prepare("SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, d_id);
        prep.setInt(2, warehouseId);
        rs = db.query(prep);
        rs.next();
        int o_id = rs.getInt(1) - 1;
        BigDecimal d_tax = rs.getBigDecimal(2);
        rs.close();
        // TODO optimizer: such cases can be optimized! A=1 AND B=A means
        // also B=1!
        //        prep = prepare("SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX "
        //                + "FROM CUSTOMER, WAREHOUSE "
        //                + "WHERE C_ID=? AND W_ID=? AND C_W_ID=W_ID AND C_D_ID=?");
        prep = prepare("SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX "
                + "FROM CUSTOMER, WAREHOUSE "
                + "WHERE C_ID=? AND C_W_ID=? AND C_W_ID=W_ID AND C_D_ID=?");
        prep.setInt(1, c_id);
        prep.setInt(2, warehouseId);
        prep.setInt(3, d_id);
        rs = db.query(prep);
        rs.next();
        BigDecimal c_discount = rs.getBigDecimal(1);
        rs.getString(2); // c_last
        rs.getString(3); // c_credit
        BigDecimal w_tax = rs.getBigDecimal(4);
        rs.close();
        BigDecimal total = new BigDecimal("0");
        for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            int ol_i_id = item_id[ol_number - 1];
            int ol_supply_w_id = supply_w_id[ol_number - 1];
            int ol_quantity = quantity[ol_number - 1];
            prep = prepare("SELECT I_PRICE, I_NAME, I_DATA "
                    + "FROM ITEM WHERE I_ID=?");
            prep.setInt(1, ol_i_id);
            rs = db.query(prep);
            if (!rs.next()) {
                if (rollback) {
                    // item not found - correct behavior
                    db.rollback();
                    return;
                }
                throw new Exception("item not found: " + ol_i_id + " "
                        + ol_supply_w_id);
            }
            BigDecimal i_price = rs.getBigDecimal(1);
            rs.getString(2); // i_name
            String i_data = rs.getString(3);
            rs.close();
            prep = prepare("SELECT S_QUANTITY, S_DATA, "
                    + "S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
                    + "S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 "
                    + "FROM STOCK WHERE S_I_ID=? AND S_W_ID=?");
            prep.setInt(1, ol_i_id);
            prep.setInt(2, ol_supply_w_id);
            rs = db.query(prep);
            if (!rs.next()) {
                if (rollback) {
                    // item not found - correct behavior
                    db.rollback();
                    return;
                }
                throw new Exception("item not found: " + ol_i_id + " "
                        + ol_supply_w_id);
            }
            int s_quantity = rs.getInt(1);
            String s_data = rs.getString(2);
            String[] s_dist = new String[10];
            for (int i = 0; i < 10; i++) {
                s_dist[i] = rs.getString(3 + i);
            }
            rs.close();
            String ol_dist_info = s_dist[d_id - 1];
            stock[ol_number - 1] = s_quantity;
            if ((i_data.indexOf("original") != -1)
                    && (s_data.indexOf("original") != -1)) {
                bg[ol_number - 1] = 'B';
            } else {
                bg[ol_number - 1] = 'G';
            }
            if (s_quantity > ol_quantity) {
                s_quantity = s_quantity - ol_quantity;
            } else {
                s_quantity = s_quantity - ol_quantity + 91;
            }
            prep = prepare("UPDATE STOCK SET S_QUANTITY=? "
                    + "WHERE S_W_ID=? AND S_I_ID=?");
            prep.setInt(1, s_quantity);
            prep.setInt(2, ol_supply_w_id);
            prep.setInt(3, ol_i_id);
            db.update(prep, "updateStock");
            BigDecimal ol_amount = new BigDecimal(ol_quantity).multiply(
                    i_price).multiply(ONE.add(w_tax).add(d_tax)).multiply(
                    ONE.subtract(c_discount));
            ol_amount = ol_amount.setScale(2, BigDecimal.ROUND_HALF_UP);
            amt[ol_number - 1] = ol_amount;
            total = total.add(ol_amount);
            prep = prepare("INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, "
                    + "OL_I_ID, OL_SUPPLY_W_ID, "
                    + "OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            prep.setInt(1, o_id);
            prep.setInt(2, d_id);
            prep.setInt(3, warehouseId);
            prep.setInt(4, ol_number);
            prep.setInt(5, ol_i_id);
            prep.setInt(6, ol_supply_w_id);
            prep.setInt(7, ol_quantity);
            prep.setBigDecimal(8, ol_amount);
            prep.setString(9, ol_dist_info);
            db.update(prep, "insertOrderLine");
        }
        prep = prepare("INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, "
                + "O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)");
        prep.setInt(1, o_id);
        prep.setInt(2, d_id);
        prep.setInt(3, warehouseId);
        prep.setInt(4, c_id);
        prep.setTimestamp(5, datetime);
        prep.setInt(6, o_ol_cnt);
        prep.setInt(7, o_all_local);
        db.update(prep, "insertOrders");
        prep = prepare("INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) "
                + "VALUES (?, ?, ?)");
        prep.setInt(1, o_id);
        prep.setInt(2, d_id);
        prep.setInt(3, warehouseId);
        db.update(prep, "insertNewOrder");
        db.commit();
    }

    private void processPayment() throws Exception {
        int d_id = random.getInt(1, bench.districtsPerWarehouse);
        int c_w_id, c_d_id;
        if (bench.warehouses > 1 && random.getInt(1, 100) <= 15) {
            do {
                c_w_id = random.getInt(1, bench.warehouses);
            } while (c_w_id != warehouseId);
            c_d_id = random.getInt(1, bench.districtsPerWarehouse);
        } else {
            c_w_id = warehouseId;
            c_d_id = d_id;
        }
        boolean byName;
        String c_last;
        int c_id = 1;
        if (random.getInt(1, 100) <= 60) {
            byName = true;
            c_last = random.getLastname(random.getNonUniform(255, 0, 999));
        } else {
            byName = false;
            c_last = "";
            c_id = random.getNonUniform(1023, 1, bench.customersPerDistrict);
        }
        BigDecimal h_amount = random.getBigDecimal(random.getInt(100, 500000),
                2);
        Timestamp datetime = new Timestamp(System.currentTimeMillis());
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_YTD = D_YTD+? "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setBigDecimal(1, h_amount);
        prep.setInt(2, d_id);
        prep.setInt(3, warehouseId);
        db.update(prep, "updateDistrict");
        prep = prepare("UPDATE WAREHOUSE SET W_YTD=W_YTD+? WHERE W_ID=?");
        prep.setBigDecimal(1, h_amount);
        prep.setInt(2, warehouseId);
        db.update(prep, "updateWarehouse");
        prep = prepare("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME "
                + "FROM WAREHOUSE WHERE W_ID=?");
        prep.setInt(1, warehouseId);
        rs = db.query(prep);
        rs.next();
        rs.getString(1); // w_street_1
        rs.getString(2); // w_street_2
        rs.getString(3); // w_city
        rs.getString(4); // w_state
        rs.getString(5); // w_zip
        String w_name = rs.getString(6);
        rs.close();
        prep = prepare("SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME "
                + "FROM DISTRICT WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, d_id);
        prep.setInt(2, warehouseId);
        rs = db.query(prep);
        rs.next();
        rs.getString(1); // d_street_1
        rs.getString(2); // d_street_2
        rs.getString(3); // d_city
        rs.getString(4); // d_state
        rs.getString(5); // d_zip
        String d_name = rs.getString(6);
        rs.close();
        BigDecimal c_balance;
        String c_credit;
        if (byName) {
            prep = prepare("SELECT COUNT(C_ID) FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=?");
            prep.setString(1, c_last);
            prep.setInt(2, c_d_id);
            prep.setInt(3, c_w_id);
            rs = db.query(prep);
            rs.next();
            int namecnt = rs.getInt(1);
            rs.close();
            if (namecnt == 0) {
                // TODO TPC-C: check if this can happen
                db.rollback();
                return;
            }
            prep = prepare("SELECT C_FIRST, C_MIDDLE, C_ID, "
                    + "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, "
                    + "C_PHONE, C_CREDIT, C_CREDIT_LIM, "
                    + "C_DISCOUNT, C_BALANCE, C_SINCE FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=? "
                    + "ORDER BY C_FIRST");
            prep.setString(1, c_last);
            prep.setInt(2, c_d_id);
            prep.setInt(3, c_w_id);
            rs = db.query(prep);
            // locate midpoint customer
            if (namecnt % 2 != 0) {
                namecnt++;
            }
            for (int n = 0; n < namecnt / 2; n++) {
                rs.next();
            }
            rs.getString(1); // c_first
            rs.getString(2); // c_middle
            c_id = rs.getInt(3);
            rs.getString(4); // c_street_1
            rs.getString(5); // c_street_2
            rs.getString(6); // c_city
            rs.getString(7); // c_state
            rs.getString(8); // c_zip
            rs.getString(9); // c_phone
            c_credit = rs.getString(10);
            rs.getString(11); // c_credit_lim
            rs.getBigDecimal(12); // c_discount
            c_balance = rs.getBigDecimal(13);
            rs.getTimestamp(14); // c_since
            rs.close();
        } else {
            prep = prepare("SELECT C_FIRST, C_MIDDLE, C_LAST, "
                    + "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, "
                    + "C_PHONE, C_CREDIT, C_CREDIT_LIM, "
                    + "C_DISCOUNT, C_BALANCE, C_SINCE FROM CUSTOMER "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setInt(1, c_id);
            prep.setInt(2, c_d_id);
            prep.setInt(3, c_w_id);
            rs = db.query(prep);
            rs.next();
            rs.getString(1); // c_first
            rs.getString(2); // c_middle
            rs.getString(3); // c_last
            rs.getString(4); // c_street_1
            rs.getString(5); // c_street_2
            rs.getString(6); // c_city
            rs.getString(7); // c_state
            rs.getString(8);//  c_zip
            rs.getString(9); //  c_phone
            c_credit = rs.getString(10);
            rs.getString(11); // c_credit_lim
            rs.getBigDecimal(12); // c_discount
            c_balance = rs.getBigDecimal(13);
            rs.getTimestamp(14); // c_since
            rs.close();
        }
        c_balance = c_balance.add(h_amount);
        if (c_credit.equals("BC")) {
            prep = prepare("SELECT C_DATA INTO FROM CUSTOMER "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setInt(1, c_id);
            prep.setInt(2, c_d_id);
            prep.setInt(3, c_w_id);
            rs = db.query(prep);
            rs.next();
            String c_data = rs.getString(1);
            rs.close();
            String c_new_data = "| " + c_id + " " + c_d_id + " " + c_w_id
                    + " " + d_id + " " + warehouseId + " " + h_amount + " "
                    + c_data;
            if (c_new_data.length() > 500) {
                c_new_data = c_new_data.substring(0, 500);
            }
            prep = prepare("UPDATE CUSTOMER SET C_BALANCE=?, C_DATA=? "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setBigDecimal(1, c_balance);
            prep.setString(2, c_new_data);
            prep.setInt(3, c_id);
            prep.setInt(4, c_d_id);
            prep.setInt(5, c_w_id);
            db.update(prep, "updateCustomer");
        } else {
            prep = prepare("UPDATE CUSTOMER SET C_BALANCE=? "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setBigDecimal(1, c_balance);
            prep.setInt(2, c_id);
            prep.setInt(3, c_d_id);
            prep.setInt(4, c_w_id);
            db.update(prep, "updateCustomer");
        }
        // MySQL bug?
//        String h_data = w_name + "    " + d_name;
        String h_data = w_name + " " + d_name;
        prep = prepare("INSERT INTO HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, "
                + "H_W_ID, H_DATE, H_AMOUNT, H_DATA) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        prep.setInt(1, c_d_id);
        prep.setInt(2, c_w_id);
        prep.setInt(3, c_id);
        prep.setInt(4, d_id);
        prep.setInt(5, warehouseId);
        prep.setTimestamp(6, datetime);
        prep.setBigDecimal(7, h_amount);
        prep.setString(8, h_data);
        db.update(prep, "insertHistory");
        db.commit();
    }

    private void processOrderStatus() throws Exception {
        int d_id = random.getInt(1, bench.districtsPerWarehouse);
        boolean byName;
        String c_last = null;
        int c_id = -1;
        if (random.getInt(1, 100) <= 60) {
            byName = true;
            c_last = random.getLastname(random.getNonUniform(255, 0, 999));
        } else {
            byName = false;
            c_id = random.getNonUniform(1023, 1, bench.customersPerDistrict);
        }
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=-1 WHERE D_ID=-1");
        db.update(prep, "updateDistrict");
        if (byName) {
            prep = prepare("SELECT COUNT(C_ID) FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=?");
            prep.setString(1, c_last);
            prep.setInt(2, d_id);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            rs.next();
            int namecnt = rs.getInt(1);
            rs.close();
            if (namecnt == 0) {
                // TODO TPC-C: check if this can happen
                db.rollback();
                return;
            }
            prep = prepare("SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_ID "
                    + "FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=? "
                    + "ORDER BY C_FIRST");
            prep.setString(1, c_last);
            prep.setInt(2, d_id);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            if (namecnt % 2 != 0) {
                namecnt++;
            }
            for (int n = 0; n < namecnt / 2; n++) {
                rs.next();
            }
            rs.getBigDecimal(1); // c_balance
            rs.getString(2); // c_first
            rs.getString(3); // c_middle
            rs.close();
        } else {
            prep = prepare("SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_LAST "
                    + "FROM CUSTOMER "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setInt(1, c_id);
            prep.setInt(2, d_id);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            rs.next();
            rs.getBigDecimal(1); // c_balance
            rs.getString(2); // c_first
            rs.getString(3); // c_middle
            rs.getString(4); // c_last
            rs.close();
        }
        prep = prepare("SELECT MAX(O_ID) "
                + "FROM ORDERS WHERE O_C_ID=? AND O_D_ID=? AND O_W_ID=?");
        prep.setInt(1, c_id);
        prep.setInt(2, d_id);
        prep.setInt(3, warehouseId);
        rs = db.query(prep);
        int o_id = -1;
        if (rs.next()) {
            o_id = rs.getInt(1);
            if (rs.wasNull()) {
                o_id = -1;
            }
        }
        rs.close();
        if (o_id != -1) {
            prep = prepare("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D "
                    + "FROM ORDERS WHERE O_ID=?");
            prep.setInt(1, o_id);
            rs = db.query(prep);
            rs.next();
            o_id = rs.getInt(1);
            rs.getInt(2); // o_carrier_id
            rs.getTimestamp(3); // o_entry_d
            rs.close();
            prep = prepare("SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, "
                    + "OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE "
                    + "WHERE OL_O_ID=? AND OL_D_ID=? AND OL_W_ID=?");
            prep.setInt(1, o_id);
            prep.setInt(2, d_id);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            while (rs.next()) {
                rs.getInt(1); // o_i_id
                rs.getInt(2); // ol_supply_w_id
                rs.getInt(3); // ol_quantity
                rs.getBigDecimal(4); // ol_amount
                rs.getTimestamp(5); // ol_delivery_d
            }
            rs.close();
        }
        db.commit();
    }

    private void processDelivery() throws Exception {
        int o_carrier_id = random.getInt(1, 10);
        Timestamp datetime = new Timestamp(System.currentTimeMillis());
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=-1 WHERE D_ID=-1");
        db.update(prep, "updateDistrict");
        for (int d_id = 1; d_id <= bench.districtsPerWarehouse; d_id++) {
            prep = prepare("SELECT MIN(NO_O_ID) FROM NEW_ORDER "
                    + "WHERE NO_D_ID=? AND NO_W_ID=?");
            prep.setInt(1, d_id);
            prep.setInt(2, warehouseId);
            rs = db.query(prep);
            int no_o_id = -1;
            if (rs.next()) {
                no_o_id = rs.getInt(1);
                if (rs.wasNull()) {
                    no_o_id = -1;
                }
            }
            rs.close();
            if (no_o_id != -1) {
                prep = prepare("DELETE FROM NEW_ORDER "
                        + "WHERE NO_O_ID=? AND NO_D_ID=? AND NO_W_ID=?");
                prep.setInt(1, no_o_id);
                prep.setInt(2, d_id);
                prep.setInt(3, warehouseId);
                db.update(prep, "deleteNewOrder");
                prep = prepare("SELECT O_C_ID FROM ORDERS "
                        + "WHERE O_ID=? AND O_D_ID=? AND O_W_ID=?");
                prep.setInt(1, no_o_id);
                prep.setInt(2, d_id);
                prep.setInt(3, warehouseId);
                rs = db.query(prep);
                rs.next();
                rs.getInt(1); // o_c_id
                rs.close();
                prep = prepare("UPDATE ORDERS SET O_CARRIER_ID=? "
                        + "WHERE O_ID=? AND O_D_ID=? AND O_W_ID=?");
                prep.setInt(1, o_carrier_id);
                prep.setInt(2, no_o_id);
                prep.setInt(3, d_id);
                prep.setInt(4, warehouseId);
                db.update(prep, "updateOrders");
                prep = prepare("UPDATE ORDER_LINE SET OL_DELIVERY_D=? "
                        + "WHERE OL_O_ID=? AND OL_D_ID=? AND OL_W_ID=?");
                prep.setTimestamp(1, datetime);
                prep.setInt(2, no_o_id);
                prep.setInt(3, d_id);
                prep.setInt(4, warehouseId);
                db.update(prep, "updateOrderLine");
                prep = prepare("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE "
                        + "WHERE OL_O_ID=? AND OL_D_ID=? AND OL_W_ID=?");
                prep.setInt(1, no_o_id);
                prep.setInt(2, d_id);
                prep.setInt(3, warehouseId);
                rs = db.query(prep);
                rs.next();
                BigDecimal ol_amount = rs.getBigDecimal(1);
                rs.close();
                prep = prepare("UPDATE CUSTOMER SET C_BALANCE=C_BALANCE+? "
                        + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
                prep.setBigDecimal(1, ol_amount);
                prep.setInt(2, no_o_id);
                prep.setInt(3, d_id);
                prep.setInt(4, warehouseId);
                db.update(prep, "updateCustomer");
            }
        }
        db.commit();
    }

    private void processStockLevel() throws Exception {
        int d_id = (terminalId % bench.districtsPerWarehouse) + 1;
        int threshold = random.getInt(10, 20);
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=-1 WHERE D_ID=-1");
        db.update(prep, "updateDistrict");

        prep = prepare("SELECT D_NEXT_O_ID FROM DISTRICT "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, d_id);
        prep.setInt(2, warehouseId);
        rs = db.query(prep);
        rs.next();
        int o_id = rs.getInt(1);
        rs.close();
//        prep = prepare("SELECT COUNT(DISTINCT S_I_ID) "
//                + "FROM ORDER_LINE, STOCK WHERE OL_W_ID=? AND "
//                + "OL_D_ID=? AND OL_O_ID<? AND "
//                + "OL_O_ID>=?-20 AND S_W_ID=? AND "
//                + "S_I_ID=OL_I_ID AND S_QUANTITY<?");
//        prep.setInt(1, warehouseId);
//        prep.setInt(2, d_id);
//        prep.setInt(3, o_id);
//        prep.setInt(4, o_id);
        prep = prepare("SELECT COUNT(DISTINCT S_I_ID) "
                + "FROM ORDER_LINE, STOCK WHERE OL_W_ID=? AND "
                + "OL_D_ID=? AND OL_O_ID<? AND "
                + "OL_O_ID>=? AND S_W_ID=? AND "
                + "S_I_ID=OL_I_ID AND S_QUANTITY<?");
        prep.setInt(1, warehouseId);
        prep.setInt(2, d_id);
        prep.setInt(3, o_id);
        prep.setInt(4, o_id-20);
        prep.setInt(5, warehouseId);
        prep.setInt(6, threshold);
        // TODO this is where HSQLDB is very slow
        rs = db.query(prep);
        rs.next();
        rs.getInt(1); // stockCount
        rs.close();
        db.commit();
    }

    private PreparedStatement prepare(String sql) throws Exception {
        PreparedStatement prep = (PreparedStatement) prepared.get(sql);
        if (prep == null) {
            prep = db.prepare(sql);
            prepared.put(sql, prep);
        }
        return prep;
    }

}
