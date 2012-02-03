/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * A tool to help an application execute multi-dimensional range queries.
 * The algorithm used is database independent, the only requirement
 * is that the engine supports a range index (for example b-tree).
 */
public class MultiDimension {

    private static final MultiDimension INSTANCE = new MultiDimension();

    private MultiDimension() {
        // don't allow construction
    }

    /**
     * Get the singleton.
     *
     * @return the singleton
     */
    public static MultiDimension getInstance() {
        return INSTANCE;
    }

    /**
     * Convert the multi-dimensional value into a one-dimensional (scalar) value.
     * This is done by interleaving the bits of the values.
     * Each values must be bigger or equal to 0. The maximum value
     * is dependent on the number of dimensions. For two keys, it is 32 bit,
     * for 3: 21 bit, 4: 16 bit, 5: 12 bit, 6: 10 bit, 7: 9 bit, 8: 8 bit.
     *
     * @param values the multi-dimensional value
     * @return the scalar value
     */
    public long interleave(int[] values) {
        int dimensions = values.length;
        int bitsPerValue = 64 / dimensions;
        // for 2 keys: 0x800000; 3: 0x
        long max = 1L << bitsPerValue;
        long x = 0;
        for (int i = 0; i < dimensions; i++) {
            long k = values[i];
            if (k < 0 || k > max) {
                throw new IllegalArgumentException("value out of range; value=" + values[i] + " min=0 max=" + max);
            }
            for (int b = 0; b < bitsPerValue; b++) {
                x |= (k & (1L << b)) << (i + (dimensions - 1) * b);
            }
        }
        if (dimensions == 2) {
            long xx = getMorton2(values[0], values[1]);
            if (xx != x) {
                throw new IllegalArgumentException("test");
            }
        }
        return x;
    }

    /**
     * Gets one of the original multi-dimensional values from a scalar value.
     *
     * @param scalar the scalar value
     * @param dimensions the number of dimensions
     * @param dim the dimension of the returned value (starting from 0)
     * @return the value
     */
    public int deinterleave(long scalar, int dimensions, int dim) {
        int bitsPerValue = 64 / dimensions;
        int value = 0;
        for (int i = 0; i < bitsPerValue; i++) {
            value |= (scalar >> (dim + (dimensions - 1) * i)) & (1L << i);
        }
        return value;
    }


//    public static int get(long z, int d) {
//        int n = 0;
//        for (int i = 0; i < 31; i++) {
//            n |= (z & (1 << (i + i + d))) >> (i + d);
//        }
//        return n;
//    }

    /**
     * Generates an optimized multi-dimensional range query.
     * The query contains parameters. It can only be used with the H2 database.
     *
     * @param table the table name
     * @param columns the list of columns
     * @param scalarColumn the column name of the computed scalar column
     * @return the query
     */
    public String generatePreparedQuery(String table, String scalarColumn, String[] columns) {
        StringBuilder buff = new StringBuilder("SELECT D.* FROM ");
        buff.append(StringUtils.quoteIdentifier(table)).
            append(" D, TABLE(_FROM_ BIGINT=?, _TO_ BIGINT=?) WHERE ").
            append(StringUtils.quoteIdentifier(scalarColumn)).
            append(" BETWEEN _FROM_ AND _TO_");
        for (String col : columns) {
            buff.append(" AND ").append(StringUtils.quoteIdentifier(col)).append("+1 BETWEEN ?+1 AND ?+1");
        }
        return buff.toString();
    }

    /**
     * Executes a prepared query that was generated using generatePreparedQuery.
     *
     * @param prep the prepared statement
     * @param min the lower values
     * @param max the upper values
     * @return the result set
     */
    public ResultSet getResult(PreparedStatement prep, int[] min, int[] max) throws SQLException {
        long[][] ranges = getMortonRanges(min, max);
        int len = ranges.length;
        Long[] from = new Long[len];
        Long[] to = new Long[len];
        for (int i = 0; i < len; i++) {
            from[i] = Long.valueOf(ranges[i][0]);
            to[i] = Long.valueOf(ranges[i][1]);
        }
        prep.setObject(1, from);
        prep.setObject(2, to);
        len = min.length;
        for (int i = 0, idx = 3; i < len; i++) {
            prep.setInt(idx++, min[i]);
            prep.setInt(idx++, max[i]);
        }
        return prep.executeQuery();
    }

    /**
     * Generates an optimized multi-dimensional range query.
     * This query is database independent, however the performance is
     * not as good as when using generatePreparedQuery
     *
     * @param table the table name
     * @param columns the list of columns
     * @param min the lower values
     * @param max the upper values
     * @param scalarColumn the column name of the computed scalar column
     * @return the query
     */
    public String generateQuery(String table, String scalarColumn, String[] columns, int[] min, int[] max) {
        long[][] ranges = getMortonRanges(min, max);
        StatementBuilder buff = new StatementBuilder("SELECT * FROM (");
        for (long[] range : ranges) {
            long minScalar = range[0];
            long maxScalar = range[1];
            buff.appendExceptFirst(" UNION ALL ");
            buff.append("SELECT * FROM ").
                append(table).
                append(" WHERE ").
                append(scalarColumn).
                append(" BETWEEN ").
                append(minScalar).
                append(" AND ").
                append(maxScalar);
        }
        buff.append(") WHERE ");
        int i = 0;
        buff.resetCount();
        for (String col : columns) {
            buff.appendExceptFirst(" AND ");
            buff.append(col).
                append(" BETWEEN ").
                append(min[i]).
                append(" AND ").
                append(max[i]);
            i++;
        }
        return buff.toString();
    }

    /**
     * Gets a list of ranges to be searched for a multi-dimensional range query
     * where min &lt;= value &lt;= max. In most cases, the ranges will be larger
     * than required in order to combine smaller ranges into one. Usually, about
     * double as much points will be included in the resulting range.
     *
     * @param min the minimum value
     * @param max the maximum value
     * @return the list of ranges
     */
    private long[][] getMortonRanges(int[] min, int[] max) {
        int len = min.length;
        if (max.length != len) {
            throw new IllegalArgumentException("dimensions mismatch");
        }
        for (int i = 0; i < len; i++) {
            if (min[i] > max[i]) {
                int temp = min[i];
                min[i] = max[i];
                max[i] = temp;
            }
        }
        int total = getSize(min, max, len);
        ArrayList<long[]> list = New.arrayList();
        addMortonRanges(list, min, max, len, 0);
        optimize(list, total);
        long[][] ranges = new long[list.size()][2];
        list.toArray(ranges);
        return ranges;
    }

    private long getMorton2(int x, int y) {
        long z = 0;
        for (int i = 0; i < 32; i++) {
            z |= (x & (1L << i)) << i;
            z |= (y & (1L << i)) << (i + 1);
        }
        return z;
    }

    private int getSize(int[] min, int[] max, int len) {
        int size = 1;
        for (int i = 0; i < len; i++) {
            int diff = max[i] - min[i];
            size *= diff + 1;
        }
        return size;
    }

    private void optimize(ArrayList<long[]> list, int total) {
        Collections.sort(list, new Comparator<long[]>() {
            public int compare(long[] a, long[] b) {
                return a[0] > b[0] ? 1 : -1;
            }
        });
        for (int minGap = 10;; minGap += minGap / 2) {
            for (int i = 0; i < list.size() - 1; i++) {
                long[] current = list.get(i);
                long[] next = list.get(i + 1);
                if (current[1] + minGap >= next[0]) {
                    current[1] = next[1];
                    list.remove(i + 1);
                    i--;
                }
            }
            int searched = 0;
            for (long[] range : list) {
                searched += range[1] - range[0] + 1;
            }
            if (searched > 2 * total || list.size() < 3 /* || minGap > total */) {
                break;
            }
        }
    }

    private void addMortonRanges(ArrayList<long[]> list, int[] min, int[] max, int len, int level) {
        if (level > 100) {
            throw new IllegalArgumentException("Stop");
        }
        int largest = 0, largestDiff = 0;
        long size = 1;
        for (int i = 0; i < len; i++) {
            int diff = max[i] - min[i];
            if (diff < 0) {
                throw new IllegalArgumentException("Stop");
            }
            size *= diff + 1;
            if (size < 0) {
                throw new IllegalArgumentException("Stop");
            }
            if (diff > largestDiff) {
                largestDiff = diff;
                largest = i;
            }
        }
        long low = interleave(min), high = interleave(max);
        if (high < low) {
            throw new IllegalArgumentException("Stop");
        }
        long range = high - low + 1;
        if (range == size) {
            long[] item = new long[] { low, high };
            list.add(item);
        } else {
            int middle = findMiddle(min[largest], max[largest]);
            int temp = max[largest];
            max[largest] = middle;
            addMortonRanges(list, min, max, len, level + 1);
            max[largest] = temp;
            temp = min[largest];
            min[largest] = middle + 1;
            addMortonRanges(list, min, max, len, level + 1);
            min[largest] = temp;
        }
    }

    private int roundUp(int x, int blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    private int findMiddle(int a, int b) {
        int diff = b - a - 1;
        if (diff == 0) {
            return a;
        }
        if (diff == 1) {
            return a + 1;
        }
        int scale = 0;
        while ((1 << scale) < diff) {
            scale++;
        }
        scale--;
        int m = roundUp(a + 2, 1 << scale) - 1;
        if (m <= a || m >= b) {
            throw new IllegalArgumentException("stop");
        }
        return m;
    }

}
