/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.h2.expression.Function;
import org.h2.result.SortOrder;
import org.h2.expression.Expression;
import org.h2.engine.Session;
import org.h2.engine.Database;
import org.h2.expression.ExpressionColumn;

/**
 * This represents a column item of an index. This is required because some
 * indexes support descending sorted columns.
 */
public class IndexTerm {

    public static IndexTerm[] fromIndexColumnArray(Database database, IndexColumn[] cols) {
      ArrayList<IndexTerm> its = new ArrayList<IndexTerm>();
      for (IndexColumn col : cols) {
        IndexTerm it = new IndexTerm();
        ExpressionColumn t = new ExpressionColumn(database, col.column);
        it.term = t;
        its.add(it);
      }
      IndexTerm[] itsArr = new IndexTerm[its.size()];
      return its.toArray(itsArr);
    }

    /**
     * The term.
     */
    public Expression term;

    /**
     * The columns in the term, or null if not set.
     */
    public Column[] columns;

    /**
     * The sort type. Ascending (the default) and descending are supported;
     * nulls can be sorted first or last.
     */
    public int sortType = SortOrder.ASCENDING;

    /**
     * Get the SQL snippet for this index term.
     *
     * @return the SQL snippet
     */
    public String getSQL() {
        StringBuilder buff = new StringBuilder(term.getSQL());
        if ((sortType & SortOrder.DESCENDING) != 0) {
            buff.append(" DESC");
        }
        if ((sortType & SortOrder.NULLS_FIRST) != 0) {
            buff.append(" NULLS FIRST");
        } else if ((sortType & SortOrder.NULLS_LAST) != 0) {
            buff.append(" NULLS LAST");
        }
        return buff.toString();
    }

    /**
     * Create an array of index columns from a list of columns. The default sort
     * type is used.
     *
     * @param columns the column list
     * @return the index column array
     */
    public static IndexTerm[] wrap(Database database, Column[] columns) {
        IndexTerm[] list = new IndexTerm[columns.length];
        for (int i = 0; i < list.length; i++) {
            list[i] = new IndexTerm();
            list[i].term = new ExpressionColumn(database, columns[i]);
            list[i].columns = new Column[] { columns[i] };
        }
        return list;
    }

    /**
     * Map the columns using the column names and the specified table.
     *
     * @param indexTerms the index list with terms set
     * @param table the table from where to map the column names to columns
     */
    public static void mapColumns(IndexTerm[] indexTerms, Table table, Session session) {

        List<Column> tblCols =  Arrays.asList(table.getColumns());
        System.out.println("tblCols: " + tblCols);


        for (IndexTerm it : indexTerms) {
            ArrayList<Column> cols = new ArrayList<Column>();

            if (it.term instanceof ExpressionColumn) {
                ExpressionColumn col = (ExpressionColumn) it.term;

                for (Column tblCol : tblCols) {
                    if (tblCol.getName().equals(col.getColumnName())) {
                        cols.add(tblCol);
                    }
                }
            } else if (it.term instanceof Function){
                Expression[] expressCols = ((Function) it.term).getArgs();
                for (Expression col: expressCols){
                    if (col instanceof ExpressionColumn){
                        col = (ExpressionColumn) col;
                        for (Column tblCol : tblCols) {
                            if (tblCol.getName().equals(col.getColumnName())) {
                                cols.add(tblCol);
                            }
                        }
                    }
                }
            }
            it.columns = cols.toArray(new Column[cols.size()]);
        }
    }


    @Override
    public String toString() {
        return "IndexTerm " + getSQL();
    }
}
