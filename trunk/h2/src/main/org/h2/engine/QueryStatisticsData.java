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
import java.util.Map;

/**
 * Maintains query statistics.
 */
public class QueryStatisticsData {

    private static final int MAX_QUERY_ENTRIES = 100;

    public static final class QueryEntry {
        public String sqlStatement;

        public long lastUpdateTime;
        public int count;
        public long executionTimeMin;
        public long executionTimeMax;
        public long executionTimeCumulative;
        public int rowCountMin;
        public int rowCountMax;
        public long rowCountCumulative;
        
        // Using Welford's method, see also
        // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
        // http://www.johndcook.com/standard_deviation.html
        public double executionTimeMean;
        public double executionTimeM2;
        public double rowCountMean;
        public double rowCountM2;
        
        public double getExecutionTimeStandardDeviation() {
            // population standard deviation
            return Math.sqrt(executionTimeM2 / count);
        }
        
        public double getRowCountStandardDeviation() {
            // population standard deviation
            return Math.sqrt(rowCountM2 / count);
        }

    }

    private final HashMap<String, QueryEntry> map = new HashMap<String, QueryEntry>();

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
            entry.count = 1;
            entry.executionTimeMin = executionTime;
            entry.executionTimeMax = executionTime;
            entry.rowCountMin = rowCount;
            entry.rowCountMax = rowCount;
            entry.executionTimeMean = executionTime;
            entry.executionTimeM2 = 0;
            entry.rowCountMean = rowCount;
            entry.rowCountM2 = 0;
            map.put(sqlStatement, entry);
        } else {
            entry.count++;
            entry.executionTimeMin = Math.min(executionTime, entry.executionTimeMin);
            entry.executionTimeMax = Math.max(executionTime, entry.executionTimeMax);
            entry.rowCountMin = Math.min(rowCount, entry.rowCountMin);
            entry.rowCountMax = Math.max(rowCount, entry.rowCountMax);
            
            double delta = rowCount - entry.rowCountMean;
            entry.rowCountMean += delta / entry.count;
            entry.rowCountM2 += delta * (rowCount - entry.rowCountMean);
            
            delta = executionTime - entry.executionTimeMean;
            entry.executionTimeMean += delta / entry.count;
            entry.executionTimeM2 += delta * (executionTime - entry.executionTimeMean);
        }
        entry.executionTimeCumulative += executionTime;        
        entry.rowCountCumulative += rowCount;
        entry.lastUpdateTime = System.currentTimeMillis();

        // Age-out the oldest entries if the map gets too big.
        // Test against 1.5 x max-size so we don't do this too often
        if (map.size() > MAX_QUERY_ENTRIES * 1.5f) {
            // Sort the entries by age
            ArrayList<QueryEntry> list = new ArrayList<QueryEntry>();
            list.addAll(map.values());
            Collections.sort(list, QUERY_ENTRY_COMPARATOR);
            // Create a set of the oldest 1/3 of the entries
            HashSet<QueryEntry> oldestSet = new HashSet<QueryEntry>(list.subList(0, list.size() / 3));
            // Loop over the map using the set and remove the oldest 1/3 of the
            // entries.
            for (Iterator<Map.Entry<String, QueryEntry>> iter = map.entrySet().iterator(); iter.hasNext();) {
                Map.Entry<String, QueryEntry> mapEntry = iter.next();
                if (oldestSet.contains(mapEntry.getValue())) {
                    iter.remove();
                }
            }
        }
    }
    
    public synchronized List<QueryEntry> getQueries() {
        // return a copy of the map so we don't have to worry about external synchronization
        ArrayList<QueryEntry> list = new ArrayList<QueryEntry>();
        list.addAll(map.values());
        // only return the newest 100 entries
        Collections.sort(list, QUERY_ENTRY_COMPARATOR);
        return list.subList(0, Math.min(list.size(), MAX_QUERY_ENTRIES));
    }

    private static final Comparator<QueryEntry> QUERY_ENTRY_COMPARATOR = new Comparator<QueryEntry>() {
        @Override
        public int compare(QueryEntry o1, QueryEntry o2) {
            return (int) Math.signum(o1.lastUpdateTime - o2.lastUpdateTime);
        }
    };
}
