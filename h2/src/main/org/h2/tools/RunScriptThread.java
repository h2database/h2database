/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;

import org.h2.constant.ErrorCode;

class RunScriptThread extends Thread {
    private int id;
    private volatile boolean stop;
    private Connection conn;
    private LinkedList queue = new LinkedList();
    
    RunScriptThread(int id, Connection conn) {
        this.id = id;
        this.conn = conn;
    }
    
    void stopThread() {
        this.stop = true;
    }
    
    void addStatement(String sql) {
        synchronized(queue) {
            queue.add(sql);
            queue.notifyAll();
        }
    }
    
    void executeAll() {
        while(true) {
            synchronized(queue) {
                if(queue.size() == 0) {
                    return;
                }
                try {
                    queue.wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }
    
    public void run() {
        while(!stop) {
            String sql;
            synchronized(queue) {
                while(queue.size() == 0) {
                    try {
                        queue.wait();
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                sql = (String) queue.removeFirst();
                queue.notifyAll();
            }
            if(sql == null) {
                continue;
            }
            try {
                conn.createStatement().execute("/*"+id+"*/" + sql);
            } catch(SQLException e) {
                switch(e.getErrorCode()) {
                case ErrorCode.LOCK_TIMEOUT_1:
                case ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1:
                    break;
                default:
                    e.printStackTrace();
                }
            }
        }
    }
}
