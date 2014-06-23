/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * Maintains query statistics.
 */
public class QueryStatisticsData {

    private static final int MAX_QUERY_ENTRIES = 100;

    private static final Comparator<QueryEntry> QUERY_ENTRY_COMPARATOR = new Comparator<QueryEntry>() {
        @Override
        public int compare(QueryEntry o1, QueryEntry o2) {
            return (int) Math.signum(o1.lastUpdateTime - o2.lastUpdateTime);
        }
    };

    private final HashMap<String, QueryEntry> map = new HashMap<String, QueryEntry>();

    public synchronized List<QueryEntry> getQueries() {
        // return a copy of the map so we don't have to
        // worry about external synchronization
        ArrayList<QueryEntry> list = new ArrayList<QueryEntry>();
        list.addAll(map.values());
        // only return the newest 100 entries
        Collections.sort(list, QUERY_ENTRY_COMPARATOR);
        return list.subList(0, Math.min(list.size(), MAX_QUERY_ENTRIES));
    }

    /**
     * Update query statistics.
     *
     * @param sqlStatement the statement being executed
     * @param executionTime the time in milliseconds the query/update took to execute
     * @param rowCount the query or update row count
     */
    public synchronized void update(String sqlStatement, long executionTime, int rowCount) {
        QueryEntry entry = map.get(sqlStatement);
        if (entry == null) {
            entry = new QueryEntry();
            entry.sqlStatement = sqlStatement;
            map.put(sqlStatement, entry);
        }
        entry.update(executionTime, rowCount);

        // Age-out the oldest entries if the map gets too big.
        // Test against 1.5 x max-size so we don't do this too often
        if (map.size() > MAX_QUERY_ENTRIES * 1.5f) {
            // Sort the entries by age
            ArrayList<QueryEntry> list = new ArrayList<QueryEntry>();
            list.addAll(map.values());
            Collections.sort(list, QUERY_ENTRY_COMPARATOR);
            // Create a set of the oldest 1/3 of the entries
            HashSet<QueryEntry> oldestSet = new HashSet<QueryEntry>(list.subList(0, list.size() / 3));
            // Loop over the map using the set and remove
            // the oldest 1/3 of the entries.
            for (Iterator<Entry<String, QueryEntry>> it = map.entrySet().iterator(); it.hasNext();) {
                Entry<String, QueryEntry> mapEntry = it.next();
                if (oldestSet.contains(mapEntry.getValue())) {
                    it.remove();
                }
            }
        }
    }

    /**
     * The collected statistics for one query.
     */
    public static final class QueryEntry {

        /**
         * The SQL statement.
         */
        public String sqlStatement;

        /**
         * The number of times the statement was executed.
         */
        public int count;

        /**
         * The last time the statistics for this entry were updated,
         * in milliseconds since 1970.
         */
        public long lastUpdateTime;

        /**
         * The minimum execution time, in milliseconds.
         */
        public long executionTimeMin;

        /**
         * The maximum execution time, in milliseconds.
         */
        public long executionTimeMax;

        /**
         * The total execution time.
         */
        public long executionTimeCumulative;

        /**
         * The minimum number of rows.
         */
        public int rowCountMin;

        /**
         * The maximum number of rows.
         */
        public int rowCountMax;

        /**
         * The total number of rows.
         */
        public long rowCountCumulative;

        /**
         * The mean execution time.
         */
        public double executionTimeMean;

        /**
         * The mean number of rows.
         */
        public double rowCountMean;

        // Using Welford's method, see also
        // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        // http://www.johndcook.com/standard_deviation.html

        private double executionTimeM2;
        private double rowCountM2;

        /**
         * Update the statistics entry.
         *
         * @param time the execution time
         * @param rows the number of rows
         */
        void update(long time, int rows) {
            count++;
            executionTimeMin = Math.min(time, executionTimeMin);
            executionTimeMax = Math.max(time, executionTimeMax);
            rowCountMin = Math.min(rows, rowCountMin);
            rowCountMax = Math.max(rows, rowCountMax);

            double delta = rows - rowCountMean;
            rowCountMean += delta / count;
            rowCountM2 += delta * (rows - rowCountMean);

            delta = time - executionTimeMean;
            executionTimeMean += delta / count;
            executionTimeM2 += delta * (time - executionTimeMean);

            executionTimeCumulative += time;
            rowCountCumulative += rows;
            lastUpdateTime = System.currentTimeMillis();

        }

        public double getExecutionTimeStandardDeviation() {
            // population standard deviation
            return Math.sqrt(executionTimeM2 / count);
        }

        public double getRowCountStandardDeviation() {
            // population standard deviation
            return Math.sqrt(rowCountM2 / count);
        }

    }

}
