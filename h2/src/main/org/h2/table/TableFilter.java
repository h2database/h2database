/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.constant.SysProperties;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Index;
import org.h2.index.IndexCondition;
import org.h2.index.IndexCursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * A table filter represents a table that is used in a query. There is one such
 * object whenever a table (or view) is used in a query. For example the
 * following query has 2 table filters: SELECT * FROM TEST T1, TEST T2.
 */
public class TableFilter implements ColumnResolver {
    private static final int BEFORE_FIRST = 0, FOUND = 1, AFTER_LAST = 2, NULL_ROW = 3;
    private final Table table;
    private final Select select;
    private String alias;
    private Session session;
    private Index index;
    private int scanCount;

    /**
     * Indicates that this filter is used in the plan.
     */
    private boolean used;

    /**
     * The filter used to walk through the index.
     */
    private final IndexCursor cursor;

    /**
     * The index conditions used for direct index lookup (start or end).
     */
    private final ArrayList<IndexCondition> indexConditions = New.arrayList();

    /**
     * Additional conditions that can't be used for index lookup, but for row
     * filter for this table (ID=ID, NAME LIKE '%X%')
     */
    private Expression filterCondition;

    /**
     * The complete join condition.
     */
    private Expression joinCondition;

    private SearchRow currentSearchRow;
    private Row current;
    private int state;
    
    /**
     * The joined table (if there is one).
     */
    private TableFilter join;
    
    /**
     * Whether this table is an outer join.
     */
    private boolean joinOuter;
    
    /**
     * The nested joined table (if there is one).
     */
    private TableFilter nestedJoin;
    
    private ArrayList<Column> naturalJoinColumns;
    private boolean foundOne;
    private Expression fullCondition;
    private final int hashCode;

    /**
     * Create a new table filter object.
     *
     * @param session the session
     * @param table the table from where to read data
     * @param alias the alias name
     * @param rightsChecked true if rights are already checked
     * @param select the select statement
     */
    public TableFilter(Session session, Table table, String alias, boolean rightsChecked, Select select) {
        this.session = session;
        this.table = table;
        this.alias = alias;
        this.select = select;
        this.cursor = new IndexCursor();
        if (!rightsChecked) {
            session.getUser().checkRight(table, Right.SELECT);
        }
        hashCode = session.nextObjectId();
    }

    public Select getSelect() {
        return select;
    }

    public Table getTable() {
        return table;
    }

    /**
     * Lock the table. This will also lock joined tables.
     *
     * @param s the session
     * @param exclusive true if an exclusive lock is required
     * @param force lock even in the MVCC mode
     */
    public void lock(Session s, boolean exclusive, boolean force) {
        table.lock(s, exclusive, force);
        if (join != null) {
            join.lock(s, exclusive, force);
        }
    }

    /**
     * Get the best plan item (index, cost) to use use for the current join
     * order.
     *
     * @param s the session
     * @param level 1 for the first table in a join, 2 for the second, and so on
     * @return the best plan item
     */
    public PlanItem getBestPlanItem(Session s, int level) {
        PlanItem item;
        if (indexConditions.size() == 0) {
            item = new PlanItem();
            item.setIndex(table.getScanIndex(s));
            item.cost = item.getIndex().getCost(s, null);
        } else {
            int len = table.getColumns().length;
            int[] masks = new int[len];
            for (IndexCondition condition : indexConditions) {
                if (condition.isEvaluatable()) {
                    if (condition.isAlwaysFalse()) {
                        masks = null;
                        break;
                    }
                    int id = condition.getColumn().getColumnId();
                    masks[id] |= condition.getMask(indexConditions);
                }
            }
            item = table.getBestPlanItem(s, masks);
            // the more index conditions, the earlier the table
            // to ensure joins without indexes are evaluated:
            // x (x.a=10); y (x.b=y.b) - see issue 113
            item.cost -= item.cost * indexConditions.size() / 100 / level;
        }
        if (nestedJoin != null) {
            setEvaluatable(nestedJoin);
            item.setNestedJoinPlan(nestedJoin.getBestPlanItem(s, level));
            int todoNestJoinCostWrong;
            // TODO optimizer: calculate cost of a join: should use separate
            // expected row number and lookup cost
            item.cost += item.cost * item.getNestedJoinPlan().cost;
        }
        if (join != null) {
            setEvaluatable(join);
            item.setJoinPlan(join.getBestPlanItem(s, level));
            // TODO optimizer: calculate cost of a join: should use separate
            // expected row number and lookup cost
            item.cost += item.cost * item.getJoinPlan().cost;
        }
        return item;
    }

    private void setEvaluatable(TableFilter join) {
        // this table filter is now evaluatable - in all sub-joins
        do {
            Expression e = join.getJoinCondition();
            if (e != null) {
                e.setEvaluatable(this, true);
            }
            TableFilter n = join.getNestedJoin();
            if (n != null) {
                setEvaluatable(n);
            }
            join = join.getJoin();
        } while (join != null);
    }

    /**
     * Set what plan item (index, cost) to use use.
     *
     * @param item the plan item
     */
    public void setPlanItem(PlanItem item) {
        setIndex(item.getIndex());
        if (nestedJoin != null) {
            if (item.getNestedJoinPlan() != null) {
                nestedJoin.setPlanItem(item.getNestedJoinPlan());
            }
        }
        if (join != null) {
            if (item.getJoinPlan() != null) {
                join.setPlanItem(item.getJoinPlan());
            }
        }
    }

    /**
     * Prepare reading rows. This method will remove all index conditions that
     * can not be used, and optimize the conditions.
     */
    public void prepare() {
        // forget all unused index conditions
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition condition = indexConditions.get(i);
            if (!condition.isAlwaysFalse()) {
                Column col = condition.getColumn();
                if (index.getColumnIndex(col) < 0) {
                    indexConditions.remove(i);
                    i--;
                }
            }
        }
        if (nestedJoin != null) {
            if (SysProperties.CHECK && nestedJoin == this) {
                DbException.throwInternalError("self join");
            }
            nestedJoin.prepare();
        }
        if (join != null) {
            if (SysProperties.CHECK && join == this) {
                DbException.throwInternalError("self join");
            }
            join.prepare();
        }
        if (filterCondition != null) {
            filterCondition = filterCondition.optimize(session);
        }
        if (joinCondition != null) {
            joinCondition = joinCondition.optimize(session);
        }
    }

    /**
     * Start the query. This will reset the scan counts.
     *
     * @param s the session
     */
    public void startQuery(Session s) {
        this.session = s;
        scanCount = 0;
        if (nestedJoin != null) {
            nestedJoin.startQuery(s);
        }
        if (join != null) {
            join.startQuery(s);
        }
    }

    /**
     * Reset to the current position.
     */
    public void reset() {
        if (nestedJoin != null) {
            nestedJoin.reset();
        }
        if (join != null) {
            join.reset();
        }
        state = BEFORE_FIRST;
        foundOne = false;
    }

    /**
     * Check if there are more rows to read.
     *
     * @return true if there are
     */
    public boolean next() {
        if (state == AFTER_LAST) {
            return false;
        } else if (state == BEFORE_FIRST) {
            cursor.find(session, indexConditions);
            if (!cursor.isAlwaysFalse()) {
                if (nestedJoin != null) {
                    nestedJoin.reset();
                }
                if (join != null) {
                    join.reset();
                }
            }
        } else {
            // state == FOUND || NULL_ROW
            // the last row was ok - try next row of the join
            if (nestedJoin != null) {
                if (join == null) {
                    if (nestedJoin.next()) {
                        return true;
                    }
                } else {
                    while (true) {
                        if (nestedJoin.next()) {
                            if (join.next()) {
                                return true;
                            }
                            join.reset();
                        }
                    }
                }
            } else {
                if (join != null && join.next()) {
                    return true;
                }
            }
        }
        while (true) {
            // go to the next row
            if (state == NULL_ROW) {
                break;
            }
            if (cursor.isAlwaysFalse()) {
                state = AFTER_LAST;
            } else {
                if ((++scanCount & 4095) == 0) {
                    checkTimeout();
                }
                if (cursor.next()) {
                    currentSearchRow = cursor.getSearchRow();
                    current = null;
                    state = FOUND;
                } else {
                    state = AFTER_LAST;
                }
            }
            // if no more rows found, try the null row (for outer joins only)
            if (state == AFTER_LAST) {
                if (joinOuter && !foundOne) {
                    setNullRow();
                } else {
                    break;
                }
            }
            if (state == FOUND && nestedJoin != null) {
                nestedJoin.reset();
                if (!nestedJoin.next()) {
                    continue;
                }
            }
            if (!isOk(filterCondition)) {
                continue;
            }
            boolean joinConditionOk = isOk(joinCondition);
            if (state == FOUND) {
                if (joinConditionOk) {
                    foundOne = true;
                } else {
                    continue;
                }
            }
            if (join != null) {
                join.reset();
                if (!join.next()) {
                    continue;
                }
            }
            // check if it's ok
            if (state == NULL_ROW || joinConditionOk) {
                return true;
            }
        }
        state = AFTER_LAST;
        return false;
    }
    
    private void setNullRow() {
        state = NULL_ROW;
        current = table.getNullRow();
        currentSearchRow = current;
        if (nestedJoin != null) {
            int todoRecurse;
            nestedJoin.setNullRow();
        }
    }

    private void checkTimeout() {
        session.checkCanceled();
        // System.out.println(this.alias+ " " + table.getName() + ": " +
        // scanCount);
    }

    private boolean isOk(Expression condition) {
        if (condition == null) {
            return true;
        }
        return Boolean.TRUE.equals(condition.getBooleanValue(session));
    }

    /**
     * Get the current row.
     *
     * @return the current row, or null
     */
    public Row get() {
        if (current == null && currentSearchRow != null) {
            current = cursor.get();
        }
        return current;
    }

    /**
     * Set the current row.
     *
     * @param current the current row
     */
    public void set(Row current) {
        // this is currently only used so that check constraints work - to set
        // the current (new) row
        this.current = current;
        this.currentSearchRow = current;
    }

    /**
     * Get the table alias name. If no alias is specified, the table name is
     * returned.
     *
     * @return the alias name
     */
    public String getTableAlias() {
        if (alias != null) {
            return alias;
        }
        return table.getName();
    }

    /**
     * Add an index condition.
     *
     * @param condition the index condition
     */
    public void addIndexCondition(IndexCondition condition) {
        indexConditions.add(condition);
    }

    /**
     * Add a filter condition.
     *
     * @param condition the condition
     * @param isJoin if this is in fact a join condition
     */
    public void addFilterCondition(Expression condition, boolean isJoin) {
        if (isJoin) {
            if (joinCondition == null) {
                joinCondition = condition;
            } else {
                joinCondition = new ConditionAndOr(ConditionAndOr.AND, joinCondition, condition);
            }
        } else {
            if (filterCondition == null) {
                filterCondition = condition;
            } else {
                filterCondition = new ConditionAndOr(ConditionAndOr.AND, filterCondition, condition);
            }
        }
    }

    /**
     * Add a joined table.
     *
     * @param filter the joined table filter
     * @param outer if this is an outer join
     * @param on the join condition
     */
    public void addJoin(TableFilter filter, boolean outer, boolean nested, Expression on) {
        if (on != null) {
            on.mapColumns(this, 0);
        }
        if (nested && SysProperties.NESTED_JOINS) {
            if (nestedJoin != null) {
                throw DbException.throwInternalError();
            }
            nestedJoin = filter;
            filter.joinOuter = outer;
            if (on != null) {
                filter.mapAndAddFilter(on);
            }
        } else {
            if (join == null) {
                join = filter;
                filter.joinOuter = outer;
                if (!SysProperties.NESTED_JOINS) {
                    if (outer) {
                        // convert all inner joins on the right hand side to outer joins
                        TableFilter f = filter.join;
                        while (f != null) {
                            f.joinOuter = true;
                            f = f.join;
                        }
                    }
                }
                if (on != null) {
                    filter.mapAndAddFilter(on);
                }
            } else {
                join.addJoin(filter, outer, nested, on);
            }
        }
    }

    /**
     * Map the columns and add the join condition.
     *
     * @param on the condition
     */
    public void mapAndAddFilter(Expression on) {
        on.mapColumns(this, 0);
        addFilterCondition(on, true);
        on.createIndexConditions(session, this);
        if (nestedJoin != null) {
            nestedJoin.mapAndAddFilter(on);
        }
        if (join != null) {
            join.mapAndAddFilter(on);
        }
    }

    public TableFilter getJoin() {
        return join;
    }

    /**
     * Check if this is an outer joined table.
     *
     * @return true if it is
     */
    public boolean isJoinOuter() {
        return joinOuter;
    }

    /**
     * Get the query execution plan text to use for this table filter.
     *
     * @param isJoin if this is a joined table
     * @return the SQL statement snippet
     */
    public String getPlanSQL(boolean isJoin) {
        StringBuilder buff = new StringBuilder();
        if (isJoin) {
            if (joinOuter) {
                buff.append("LEFT OUTER JOIN ");
            } else {
                buff.append("INNER JOIN ");
            }
        }
        if (nestedJoin != null) {
            buff.append("(");
        }
        buff.append(table.getSQL());
        if (alias != null) {
            buff.append(' ').append(Parser.quoteIdentifier(alias));
        }
        if (index != null) {
            buff.append(" /* ");
            StatementBuilder planBuff = new StatementBuilder();
            planBuff.append(index.getPlanSQL());
            if (indexConditions.size() > 0) {
                planBuff.append(": ");
                for (IndexCondition condition : indexConditions) {
                    planBuff.appendExceptFirst(" AND ");
                    planBuff.append(condition.getSQL());
                }
            }
            String plan = StringUtils.quoteRemarkSQL(planBuff.toString());
            buff.append(plan).append(" */");
        }
        if (nestedJoin != null) {
            buff.append('\n');
            buff.append(nestedJoin.getPlanSQL(true));
            buff.append(")");
        }
        if (isJoin) {
            buff.append(" ON ");
            if (joinCondition == null) {
                // need to have a ON expression, otherwise the nesting is
                // unclear
                buff.append("1=1");
            } else {
                buff.append(StringUtils.unEnclose(joinCondition.getSQL()));
            }
        }
        if (filterCondition != null) {
            buff.append(" /* WHERE ");
            String condition = StringUtils.unEnclose(filterCondition.getSQL());
            condition = StringUtils.quoteRemarkSQL(condition);
            buff.append(condition).append(" */");
        }
        if (scanCount > 0) {
            buff.append(" /* scanCount: ").append(scanCount).append(" */");
        }
        return buff.toString();
    }

    /**
     * Remove all index conditions that are not used by the current index.
     */
    void removeUnusableIndexConditions() {
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition cond = indexConditions.get(i);
            if (!cond.isEvaluatable()) {
                indexConditions.remove(i--);
            }
        }
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
        cursor.setIndex(index);
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public boolean isUsed() {
        return used;
    }

    /**
     * Set the session of this table filter.
     *
     * @param session the new session
     */
    void setSession(Session session) {
        this.session = session;
    }

    /**
     * Remove the joined table
     */
    public void removeJoin() {
        this.join = null;
    }

    public Expression getJoinCondition() {
        return joinCondition;
    }

    /**
     * Remove the join condition.
     */
    public void removeJoinCondition() {
        this.joinCondition = null;
    }

    public Expression getFilterCondition() {
        return filterCondition;
    }

    /**
     * Remove the filter condition.
     */
    public void removeFilterCondition() {
        this.filterCondition = null;
    }

    public void setFullCondition(Expression condition) {
        this.fullCondition = condition;
        if (join != null) {
            join.setFullCondition(condition);
        }
    }

    /**
     * Optimize the full condition. This will add the full condition to the
     * filter condition.
     *
     * @param fromOuterJoin if this method was called from an outer joined table
     */
    void optimizeFullCondition(boolean fromOuterJoin) {
        if (fullCondition != null) {
            fullCondition.addFilterConditions(this, fromOuterJoin || joinOuter);
            if (nestedJoin != null) {
                nestedJoin.optimizeFullCondition(fromOuterJoin || joinOuter);
            }
            if (join != null) {
                join.optimizeFullCondition(fromOuterJoin || joinOuter);
            }
        }
    }

    /**
     * Update the filter and join conditions of this and all joined tables with
     * the information that the given table filter can now return rows or not.
     *
     * @param filter the table filter
     * @param b the new flag
     */
    public void setEvaluatable(TableFilter filter, boolean b) {
        if (filterCondition != null) {
            filterCondition.setEvaluatable(filter, b);
        }
        if (joinCondition != null) {
            joinCondition.setEvaluatable(filter, b);
        }
        if (nestedJoin != null) {
            nestedJoin.setEvaluatable(filter, b);
        }
        if (join != null) {
            join.setEvaluatable(filter, b);
        }
    }

    public String getSchemaName() {
        return table.getSchema().getName();
    }

    public Column[] getColumns() {
        return table.getColumns();
    }

    /**
     * Get the system columns that this table understands. This is used for
     * compatibility with other databases. The columns are only returned if the
     * current mode supports system columns.
     *
     * @return the system columns
     */
    public Column[] getSystemColumns() {
        if (!session.getDatabase().getMode().systemColumns) {
            return null;
        }
        Column[] sys = new Column[3];
        sys[0] = new Column("oid", Value.INT);
        sys[0].setTable(table, 0);
        sys[1] = new Column("ctid", Value.STRING);
        sys[1].setTable(table, 0);
        sys[2] = new Column("CTID", Value.STRING);
        sys[2].setTable(table, 0);
        return sys;
    }

    public Value getValue(Column column) {
        if (currentSearchRow == null) {
            return null;
        }
        int columnId = column.getColumnId();
        if (current == null) {
            Value v = currentSearchRow.getValue(columnId);
            if (v != null) {
                return v;
            }
            current = cursor.get();
        }
        return current.getValue(columnId);
    }

    public TableFilter getTableFilter() {
        return this;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Expression optimize(ExpressionColumn expressionColumn, Column column) {
        return expressionColumn;
    }

    public String toString() {
        return alias != null ? alias : table.toString();
    }

    /**
     * Add a column to the natural join key column list.
     *
     * @param c the column to add
     */
    public void addNaturalJoinColumn(Column c) {
        if (naturalJoinColumns == null) {
            naturalJoinColumns = New.arrayList();
        }
        naturalJoinColumns.add(c);
    }

    /**
     * Check if the given column is a natural join column.
     *
     * @param c the column to check
     * @return true if this is a joined natural join column
     */
    public boolean isNaturalJoinColumn(Column c) {
        return naturalJoinColumns != null && naturalJoinColumns.indexOf(c) >= 0;
    }

    public int hashCode() {
        return hashCode;
    }

    /**
     * Are there any index conditions that involve IN(...).
     *
     * @return whether there are IN(...) comparisons
     */
    public boolean hasInComparisons() {
        for (IndexCondition cond : indexConditions) {
            int compareType = cond.getCompareType();
            if (compareType == Comparison.IN_QUERY || compareType == Comparison.IN_LIST) {
                return true;
            }
        }
        return false;
    }

    /**
     * Add the current row to the array, if there is a current row.
     *
     * @param rows the rows to lock
     */
    public void lockRow(ArrayList<Row> rows) {
        if (state == FOUND) {
            rows.add(get());
        }
    }

    /**
     * Lock the given rows.
     *
     * @param forUpdateRows the rows to lock
     */
    public void lockRows(ArrayList<Row> forUpdateRows) {
        for (Row row : forUpdateRows) {
            table.removeRow(session, row);
            table.addRow(session, row);
        }
    }

    public TableFilter getNestedJoin() {
        return nestedJoin;
    }

}
