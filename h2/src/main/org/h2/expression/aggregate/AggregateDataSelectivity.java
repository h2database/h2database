/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.util.IntIntHashMap;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * Data stored while calculating a SELECTIVITY aggregate.
 */
public class AggregateDataSelectivity extends AggregateData {

    private long count, distinctCount;
    private IntIntHashMap distinctHashes;

    /**
     * Creates new instance of data for SELECTIVITY aggregate.
     */
    public AggregateDataSelectivity() {
    }

    @Override
    public void add(Database database, Value v) {
        count++;
        if (distinctHashes == null) {
            distinctHashes = new IntIntHashMap(false);
        } else {
            int size = distinctHashes.size();
            if (size >= Constants.SELECTIVITY_DISTINCT_COUNT) {
                distinctHashes.clear();
                distinctCount += size;
            }
        }
        // the value -1 is not supported
        distinctHashes.put(v.hashCode(), 1);
    }

    @Override
    public Value getValue(Database database, int dataType) {
        int s;
        if (count == 0) {
            s = 0;
        } else {
            s = (int) (100 * (distinctCount + distinctHashes.size()) / count);
            if (s <= 0) {
                s = 1;
            }
        }
        return ValueInt.get(s).convertTo(dataType);
    }

}
