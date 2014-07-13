/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Map.Entry;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreTool;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests the MVStoreTool class.
 */
public class TestMVStoreTool extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.config.big = true;
        test.test();
    }

    @Override
    public void test() throws Exception {
        ; // TODO work in progress
        // testCompress();
    }

    private void testCompress() {
        String fileName = getBaseDir() + "/testCompress.h3";
        FileUtils.delete(fileName);
        // store with a very small page size, to make sure
        // there are many leaf pages
        MVStore s = new MVStore.Builder().
                pageSplitSize(100).
                fileName(fileName).autoCommitDisabled().open();
        MVMap<Integer, Integer> map = s.openMap("data");
        for (int i = 0; i < 10; i++) {
            map.put(i, i * 10);
            if (i % 3 == 0) {
                s.commit();
            }
        }
        s.close();
//        MVStoreTool.dump(fileName);
//        MVStoreTool.dump(fileName + ".new");
        MVStoreTool.compress(fileName, fileName + ".new");
        MVStore s1 = new MVStore.Builder().
                fileName(fileName).readOnly().open();
        MVStore s2 = new MVStore.Builder().
                fileName(fileName + ".new").readOnly().open();
        assertEquals(s1, s2);
    }

    private void assertEquals(MVStore a, MVStore b) {
        assertEquals(a.getMapNames().size(), b.getMapNames().size());
        for (String mapName : a.getMapNames()) {
            MVMap<?, ?> ma = a.openMap(mapName);
            MVMap<?, ?> mb = a.openMap(mapName);
            assertEquals(ma.sizeAsLong(), mb.sizeAsLong());
            for (Entry<?, ?> e : ma.entrySet()) {
                Object x = mb.get(e.getKey());
                assertEquals(e.getValue().toString(), x.toString());
            }
        }
    }

}
