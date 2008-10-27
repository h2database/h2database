/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.util.Random;

import org.h2.test.TestBase;

/**
 * Tests the multi-threaded mode.
 */
public class TestMultiThreaded extends TestBase {

    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        if (config.mvcc) {
            return;
        }
        int test;
        deleteDb("multiThreaded");
        int size = getSize(2, 4);
        Connection[] conn = new Connection[size];
        for(int i=0; i<size; i++) {
            conn[i] = getConnection("multiThreaded;MULTI_THREADED=1");
        }
        Random random = new Random(1);
        for(int i=0; i<size; i++) {
            conn[i].close();
        }
    }

}
