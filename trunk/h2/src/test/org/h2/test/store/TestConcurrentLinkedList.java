/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

import org.h2.mvstore.ConcurrentLinkedList;
import org.h2.test.TestBase;
import org.h2.util.Task;

public class TestConcurrentLinkedList extends TestBase {

    /**
     * Run just this test.
     * 
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testPerformance(true);
        testPerformance(false);
        testPerformance(true);
        testPerformance(false);
        testPerformance(true);
        testPerformance(false);
        testConcurrent();
        testRandomized();
    }
    
    private void testPerformance(final boolean stock) {
        System.out.print(stock ? "stock " : "custom ");
        long start = System.currentTimeMillis();
        final ConcurrentLinkedList<Integer> test = new ConcurrentLinkedList<Integer>();
        final LinkedList<Integer> x = new LinkedList<Integer>();
        Task task = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    if (stock) {
                        synchronized (x) {
                            x.peekFirst();
                        }
                    } else {
                        test.peekFirst();
                    }
                }
            }
        };
        task.execute();
        test.add(-1);
        x.add(-1);
        for (int i = 0; i < 10000000; i++) {
            Integer value = i;
            if (stock) {
                synchronized (x) {
                    Integer f = x.peekLast();
                    if (!f.equals(value)) {
                        x.add(i);
                    }
                }
                synchronized (x) {
                    if (x.peek() != x.peekLast()) {
                        x.peek();
                        x.removeFirst();
                    }
                }
            } else {
                Integer f = test.peekLast();
                if (!f.equals(value)) {
                    test.add(i);
                }
                if (test.peekFirst() != test.peekLast()) {
                    f = test.peekFirst();
                    test.removeFirst(f);
                }
            }
        }
        task.get();
        System.out.println(System.currentTimeMillis() - start);
    }

    private void testConcurrent() {
        // TODO Auto-generated method stub
        
    }

    private void testRandomized() {
        Random r = new Random(0);
        for (int i = 0; i < 100; i++) {
            ConcurrentLinkedList<Integer> test = new ConcurrentLinkedList<Integer>();
            LinkedList<Integer> x = new LinkedList<Integer>();
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < 10000; j++) {
                int opType = r.nextInt(10);
                switch (opType) {
                case 0: {
                    int value = r.nextInt(100);
                    buff.append(" add " + value);
                    test.add(value);
                    x.add(value);
                    break;
                }
                case 1: {
                    Integer value = x.peek();
                    buff.append(" peek");
                    if (value == null) {
                        assertNull(test.peekFirst());
                    } else {
                        assertEquals(value.intValue(), test.peekFirst().intValue());
                    }
                    break;
                }
                case 2: {
                    Integer value = x.peekLast();
                    buff.append(" peeLast");
                    if (value == null) {
                        assertNull(test.peekLast());
                    } else {
                        assertEquals(value.intValue(), test.peekLast()
                                .intValue());
                    }
                    break;
                }
                case 3: {
                    if (x.size() >= 2) {
                        Integer value = x.peek();
                        if (value != null && r.nextBoolean()) {
                            buff.append(" removeFirst");
                            x.removeFirst();
                            test.removeFirst(value);
                        } else {
                            test.removeFirst(-1);
                        }
                    }
                    break;
                }
                case 4: {
                    if (x.size() >= 2) {
                        Integer value = x.peekLast();
                        if (value != null && r.nextBoolean()) {
                            buff.append(" removeLast");
                            x.removeLast();
                            test.removeLast(value);
                        } else {
                            test.removeLast(-1);
                        }
                    }
                    break;
                }
                }
                assertEquals(toString(x.iterator()), toString(test.iterator()));
            }
        }
    }
    
    private static <T> String toString(Iterator<T> it) {
        StringBuilder buff = new StringBuilder();
        while (it.hasNext()) {
            buff.append(' ').append(it.next());
        }
        return buff.toString();
    }

}
