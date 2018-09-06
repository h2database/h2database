/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;

import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Index;
import org.h2.mvstore.db.MVSpatialIndex;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueNull;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataST_Extent extends AggregateData {

    private Envelope envelope;

    /**
     * Get the index (if any) for the column specified in the geometry
     * aggregate.
     *
     * @param on
     *            the expression (usually a column expression)
     * @return the index, or null
     */
    static Index getGeometryColumnIndex(Expression on) {
        if (on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) on;
            Column column = col.getColumn();
            if (column.getType() == Value.GEOMETRY) {
                TableFilter filter = col.getTableFilter();
                if (filter != null) {
                    ArrayList<Index> indexes = filter.getTable().getIndexes();
                    if (indexes != null) {
                        for (int i = 1, size = indexes.size(); i < size; i++) {
                            Index index = indexes.get(i);
                            if (index instanceof MVSpatialIndex) {
                                return index;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        if (envelope == null) {
            envelope = new Envelope();
        }
        envelope.expandToInclude(((ValueGeometry) v.convertTo(Value.GEOMETRY)).getEnvelopeNoCopy());
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (envelope == null || envelope.isNull()) {
            return ValueNull.INSTANCE;
        }
        return ValueGeometry.getFromGeometry(new GeometryFactory().toGeometry(envelope));
    }

}
