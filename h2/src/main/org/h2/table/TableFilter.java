/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexCondition;
import org.h2.index.IndexCursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.util.DoneFuture;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A table filter represents a table that is used in a query. There is one such
 * object whenever a table (or view) is used in a query. For example the
 * following query has 2 table filters: SELECT * FROM TEST T1, TEST T2.
 */
public class TableFilter implements ColumnResolver {

    private static final int BEFORE_FIRST = 0, FOUND = 1, AFTER_LAST = 2,
            NULL_ROW = 3;

    private static final Cursor EMPTY_CURSOR = new Cursor() {
        @Override
        public boolean previous() {
            return false;
        }
        
        @Override
        public boolean next() {
            return false;
        }
        
        @Override
        public SearchRow getSearchRow() {
            return null;
        }
        
        @Override
        public Row get() {
            return null;
        }
        
        @Override
        public String toString() {
            return "EMPTY_CURSOR";
        }
    }; 
    
    /**
     * Whether this is a direct or indirect (nested) outer join
     */
    protected boolean joinOuterIndirect;

    private Session session;

    private final Table table;
    private final Select select;
    private String alias;
    private Index index;
    private int[] masks;
    private int scanCount;
    private boolean evaluatable;

    /**
     * Batched join support.
     */
    private JoinBatch joinBatch;
    private JoinFilter joinFilter;
    
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
     * Whether this is an outer join.
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
    public TableFilter(Session session, Table table, String alias,
            boolean rightsChecked, Select select) {
        this.session = session;
        this.table = table;
        this.alias = alias;
        this.select = select;
        this.cursor = new IndexCursor(this);
        if (!rightsChecked) {
            session.getUser().checkRight(table, Right.SELECT);
        }
        hashCode = session.nextObjectId();
    }

    @Override
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
     * @param forceLockEvenInMvcc lock even in the MVCC mode
     */
    public void lock(Session s, boolean exclusive, boolean forceLockEvenInMvcc) {
        table.lock(s, exclusive, forceLockEvenInMvcc);
        if (join != null) {
            join.lock(s, exclusive, forceLockEvenInMvcc);
        }
    }

    /**
     * Get the best plan item (index, cost) to use use for the current join
     * order.
     *
     * @param s the session
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @return the best plan item
     */
    public PlanItem getBestPlanItem(Session s, TableFilter[] filters, int filter) {
        PlanItem item;
        if (indexConditions.size() == 0) {
            item = new PlanItem();
            item.setIndex(table.getScanIndex(s));
            item.cost = item.getIndex().getCost(s, null, filters, filter, null);
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
                    if (id >= 0) {
                        masks[id] |= condition.getMask(indexConditions);
                    }
                }
            }
            SortOrder sortOrder = null;
            if (select != null) {
                sortOrder = select.getSortOrder();
            }
            item = table.getBestPlanItem(s, masks, filters, filter, sortOrder);
            item.setMasks(masks);
            // The more index conditions, the earlier the table.
            // This is to ensure joins without indexes run quickly:
            // x (x.a=10); y (x.b=y.b) - see issue 113
            item.cost -= item.cost * indexConditions.size() / 100 / (filter + 1);
        }
        if (nestedJoin != null) {
            setEvaluatable(nestedJoin);
            item.setNestedJoinPlan(nestedJoin.getBestPlanItem(s, filters, filter));
            // TODO optimizer: calculate cost of a join: should use separate
            // expected row number and lookup cost
            item.cost += item.cost * item.getNestedJoinPlan().cost;
        }
        if (join != null) {
            setEvaluatable(join);
            filter += nestedJoin == null ? 1 : 2;
            assert filters[filter] == join;
            item.setJoinPlan(join.getBestPlanItem(s, filters, filter));
            // TODO optimizer: calculate cost of a join: should use separate
            // expected row number and lookup cost
            item.cost += item.cost * item.getJoinPlan().cost;
        }
        return item;
    }

    private void setEvaluatable(TableFilter join) {
        if (session.getDatabase().getSettings().nestedJoins) {
            setEvaluatable(true);
            return;
        }
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
     * Set what plan item (index, cost, masks) to use.
     *
     * @param item the plan item
     */
    public void setPlanItem(PlanItem item) {
        if (item == null) {
            // invalid plan, most likely because a column wasn't found
            // this will result in an exception later on
            return;
        }
        setIndex(item.getIndex());
        masks = item.getMasks();
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
        // the indexConditions list may be modified here
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition condition = indexConditions.get(i);
            if (!condition.isAlwaysFalse()) {
                Column col = condition.getColumn();
                if (col.getColumnId() >= 0) {
                    if (index.getColumnIndex(col) < 0) {
                        indexConditions.remove(i);
                        i--;
                    }
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
     * @return join batch if query runs over index which supports batched lookups, null otherwise
     */
    public JoinBatch startQuery(Session s) {
        joinBatch = null;
        joinFilter = null;
        this.session = s;
        scanCount = 0;
        if (nestedJoin != null) {
            nestedJoin.startQuery(s);
        }
        JoinBatch batch = null;
        if (join != null) {
            batch = join.startQuery(s);
        }
        IndexLookupBatch lookupBatch = null;
        if (batch == null && select != null && select.getTopTableFilter() != this) {
            lookupBatch = index.createLookupBatch(this);
            if (lookupBatch != null) {
                batch = new JoinBatch(join);
            }
        }
        if (batch != null) {
            if (nestedJoin != null) {
                throw DbException.getUnsupportedException("nested join with batched index");
            }
            if (lookupBatch == null) {
                lookupBatch = index.createLookupBatch(this);
            }
            joinBatch = batch;
            joinFilter = batch.register(this, lookupBatch);
        }
        return batch;
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
        if (joinBatch != null) {
            // will happen only on topTableFilter since jbatch.next does not call join.next()
            return joinBatch.next();
        }
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
            if (join != null && join.next()) {
                return true;
            }
        }
        while (true) {
            // go to the next row
            if (state == NULL_ROW) {
                break;
            }
            if (cursor.isAlwaysFalse()) {
                state = AFTER_LAST;
            } else if (nestedJoin != null) {
                if (state == BEFORE_FIRST) {
                    state = FOUND;
                }
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
            if (nestedJoin != null && state == FOUND) {
                if (!nestedJoin.next()) {
                    state = AFTER_LAST;
                    if (joinOuter && !foundOne) {
                        // possibly null row
                    } else {
                        continue;
                    }
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

    /**
     * Set the state of this and all nested tables to the NULL row.
     */
    protected void setNullRow() {
        state = NULL_ROW;
        current = table.getNullRow();
        currentSearchRow = current;
        if (nestedJoin != null) {
            nestedJoin.visit(new TableFilterVisitor() {
                @Override
                public void accept(TableFilter f) {
                    f.setNullRow();
                }
            });
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
    @Override
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
                joinCondition = new ConditionAndOr(ConditionAndOr.AND,
                        joinCondition, condition);
            }
        } else {
            if (filterCondition == null) {
                filterCondition = condition;
            } else {
                filterCondition = new ConditionAndOr(ConditionAndOr.AND,
                        filterCondition, condition);
            }
        }
    }

    /**
     * Add a joined table.
     *
     * @param filter the joined table filter
     * @param outer if this is an outer join
     * @param nested if this is a nested join
     * @param on the join condition
     */
    public void addJoin(TableFilter filter, boolean outer, boolean nested,
            final Expression on) {
        if (on != null) {
            on.mapColumns(this, 0);
            if (session.getDatabase().getSettings().nestedJoins) {
                visit(new TableFilterVisitor() {
                    @Override
                    public void accept(TableFilter f) {
                        on.mapColumns(f, 0);
                    }
                });
                filter.visit(new TableFilterVisitor() {
                    @Override
                    public void accept(TableFilter f) {
                        on.mapColumns(f, 0);
                    }
                });
            }
        }
        if (nested && session.getDatabase().getSettings().nestedJoins) {
            if (nestedJoin != null) {
                throw DbException.throwInternalError();
            }
            nestedJoin = filter;
            filter.joinOuter = outer;
            if (outer) {
                visit(new TableFilterVisitor() {
                    @Override
                    public void accept(TableFilter f) {
                        f.joinOuterIndirect = true;
                    }
                });
            }
            if (on != null) {
                filter.mapAndAddFilter(on);
            }
        } else {
            if (join == null) {
                join = filter;
                filter.joinOuter = outer;
                if (session.getDatabase().getSettings().nestedJoins) {
                    if (outer) {
                        filter.visit(new TableFilterVisitor() {
                            @Override
                            public void accept(TableFilter f) {
                                f.joinOuterIndirect = true;
                            }
                        });
                    }
                } else {
                    if (outer) {
                        // convert all inner joins on the right hand side to
                        // outer joins
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
            on.mapColumns(nestedJoin, 0);
            on.createIndexConditions(session, nestedJoin);
        }
        if (join != null) {
            join.mapAndAddFilter(on);
        }
    }

    public TableFilter getJoin() {
        return join;
    }

    /**
     * Whether this is an outer joined table.
     *
     * @return true if it is
     */
    public boolean isJoinOuter() {
        return joinOuter;
    }

    /**
     * Whether this is indirectly an outer joined table (nested within an inner
     * join).
     *
     * @return true if it is
     */
    public boolean isJoinOuterIndirect() {
        return joinOuterIndirect;
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
            StringBuffer buffNested = new StringBuffer();
            TableFilter n = nestedJoin;
            do {
                buffNested.append(n.getPlanSQL(n != nestedJoin));
                buffNested.append('\n');
                n = n.getJoin();
            } while (n != null);
            String nested = buffNested.toString();
            boolean enclose = !nested.startsWith("(");
            if (enclose) {
                buff.append("(\n");
            }
            buff.append(StringUtils.indent(nested, 4, false));
            if (enclose) {
                buff.append(')');
            }
            if (isJoin) {
                buff.append(" ON ");
                if (joinCondition == null) {
                    // need to have a ON expression,
                    // otherwise the nesting is unclear
                    buff.append("1=1");
                } else {
                    buff.append(StringUtils.unEnclose(joinCondition.getSQL()));
                }
            }
            return buff.toString();
        }
        buff.append(table.getSQL());
        if (alias != null) {
            buff.append(' ').append(Parser.quoteIdentifier(alias));
        }
        if (index != null) {
            buff.append('\n');
            StatementBuilder planBuff = new StatementBuilder();
            planBuff.append(index.getPlanSQL());
            if (indexConditions.size() > 0) {
                planBuff.append(": ");
                for (IndexCondition condition : indexConditions) {
                    planBuff.appendExceptFirst("\n    AND ");
                    planBuff.append(condition.getSQL());
                }
            }
            String plan = StringUtils.quoteRemarkSQL(planBuff.toString());
            if (plan.indexOf('\n') >= 0) {
                plan += "\n";
            }
            buff.append(StringUtils.indent("/* " + plan + " */", 4, false));
        }
        if (isJoin) {
            buff.append("\n    ON ");
            if (joinCondition == null) {
                // need to have a ON expression, otherwise the nesting is
                // unclear
                buff.append("1=1");
            } else {
                buff.append(StringUtils.unEnclose(joinCondition.getSQL()));
            }
        }
        if (filterCondition != null) {
            buff.append('\n');
            String condition = StringUtils.unEnclose(filterCondition.getSQL());
            condition = "/* WHERE " + StringUtils.quoteRemarkSQL(condition) + "\n*/";
            buff.append(StringUtils.indent(condition, 4, false));
        }
        if (scanCount > 0) {
            buff.append("\n    /* scanCount: ").append(scanCount).append(" */");
        }
        return buff.toString();
    }

    /**
     * Remove all index conditions that are not used by the current index.
     */
    void removeUnusableIndexConditions() {
        // the indexConditions list may be modified here
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition cond = indexConditions.get(i);
            if (!cond.isEvaluatable()) {
                indexConditions.remove(i--);
            }
        }
    }

    public int[] getMasks() {
        return masks;
    }

    public ArrayList<IndexCondition> getIndexConditions() {
        return indexConditions;
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
     * the information that the given table filter and all nested filter can now
     * return rows or not.
     *
     * @param filter the table filter
     * @param b the new flag
     */
    public void setEvaluatable(TableFilter filter, boolean b) {
        filter.setEvaluatable(b);
        if (filterCondition != null) {
            filterCondition.setEvaluatable(filter, b);
        }
        if (joinCondition != null) {
            joinCondition.setEvaluatable(filter, b);
        }
        if (nestedJoin != null) {
            // don't enable / disable the nested join filters
            // if enabling a filter in a joined filter
            if (this == filter) {
                nestedJoin.setEvaluatable(nestedJoin, b);
            }
        }
        if (join != null) {
            join.setEvaluatable(filter, b);
        }
    }

    public void setEvaluatable(boolean evaluatable) {
        this.evaluatable = evaluatable;
    }

    @Override
    public String getSchemaName() {
        return table.getSchema().getName();
    }

    @Override
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
    @Override
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

    @Override
    public Column getRowIdColumn() {
        if (session.getDatabase().getSettings().rowId) {
            return table.getRowIdColumn();
        }
        return null;
    }

    @Override
    public Value getValue(Column column) {
        if (joinBatch != null) {
            return joinBatch.getValue(joinFilter, column);
        }
        if (currentSearchRow == null) {
            return null;
        }
        int columnId = column.getColumnId();
        if (columnId == -1) {
            return ValueLong.get(currentSearchRow.getKey());
        }
        if (current == null) {
            Value v = currentSearchRow.getValue(columnId);
            if (v != null) {
                return v;
            }
            current = cursor.get();
            if (current == null) {
                return ValueNull.INSTANCE;
            }
        }
        return current.getValue(columnId);
    }

    @Override
    public TableFilter getTableFilter() {
        return this;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public Expression optimize(ExpressionColumn expressionColumn, Column column) {
        return expressionColumn;
    }

    @Override
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
        return naturalJoinColumns != null && naturalJoinColumns.contains(c);
    }

    @Override
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
    public void lockRowAdd(ArrayList<Row> rows) {
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
            Row newRow = row.getCopy();
            table.removeRow(session, row);
            session.log(table, UndoLogRecord.DELETE, row);
            table.addRow(session, newRow);
            session.log(table, UndoLogRecord.INSERT, newRow);
        }
    }

    public TableFilter getNestedJoin() {
        return nestedJoin;
    }

    /**
     * Visit this and all joined or nested table filters.
     *
     * @param visitor the visitor
     */
    public void visit(TableFilterVisitor visitor) {
        TableFilter f = this;
        do {
            visitor.accept(f);
            TableFilter n = f.nestedJoin;
            if (n != null) {
                n.visit(visitor);
            }
            f = f.join;
        } while (f != null);
    }

    public boolean isEvaluatable() {
        return evaluatable;
    }

    public Session getSession() {
        return session;
    }

    /**
     * A visitor for table filters.
     */
    public interface TableFilterVisitor {

        /**
         * This method is called for each nested or joined table filter.
         *
         * @param f the filter
         */
        void accept(TableFilter f);
    }

    /**
     * Support for asynchronous batched index lookups on joins.
     * 
     * @see Index#findBatched(TableFilter, java.util.Collection)
     * @see Index#getPreferedLookupBatchSize()
     * 
     * @author Sergi Vladykin
     */
    private static final class JoinBatch {
        int filtersCount;
        JoinFilter[] filters;
        JoinFilter top;
        
        boolean started;
        
        JoinRow current;
        boolean found;
        
        /**
         * This filter joined after this batched join and can be used normally.
         */
        final TableFilter additionalFilter;
        
        /**
         * @param additionalFilter table filter after this batched join.
         */
        private JoinBatch(TableFilter additionalFilter) {
            this.additionalFilter = additionalFilter;
        }
        
        /**
         * @param filter table filter
         * @param lookupBatch lookup batch
         */
        private JoinFilter register(TableFilter filter, IndexLookupBatch lookupBatch) {
            assert filter != null;
            filtersCount++;
            return top = new JoinFilter(lookupBatch, filter, top);
        }
        
        /**
         * @param filterId table filter id
         * @param column column
         * @return column value for current row
         */
        private Value getValue(JoinFilter filter, Column column) {
            Object x = current.row(filter.id);
            assert x != null;
            Row row = current.isRow(filter.id) ? (Row) x : ((Cursor) x).get();
            int columnId = column.getColumnId();
            if (columnId == -1) {
                return ValueLong.get(row.getKey());
            }
            Value value = row.getValue(column.getColumnId());
            if (value == null) {
                throw DbException.throwInternalError("value is null: " + column + " " + row);
            }
            return value;
        }
        
        private void start() {
            if (filtersCount > 32) {
                // This is because we store state in a 64 bit field, 2 bits per joined table.
                throw DbException.getUnsupportedException("Too many tables in join (at most 32 supported).");
            }

            // fill filters
            filters = new JoinFilter[filtersCount];
            JoinFilter jf = top;
            for (int i = 0; i < filtersCount; i++) {
                filters[jf.id = i] = jf;
                jf = jf.join;
            }
            // initialize current row
            current = new JoinRow(new Object[filtersCount]);
            current.updateRow(top.id, top.filter.cursor, JoinRow.S_NULL, JoinRow.S_CURSOR);

            // initialize top cursor
            top.filter.cursor.find(top.filter.session, top.filter.indexConditions);

            // we need fake first row because batchedNext always will move to the next row
            JoinRow fake = new JoinRow(null);
            fake.next = current;
            current = fake;
        }

        private boolean next() {
            if (!started) {
                start();
                started = true;
            }
            if (additionalFilter == null) {
                if (batchedNext()) {
                    assert current.isComplete();
                    return true;
                }
                return false;
            }
            for (;;) {
                if (!found) {
                    if (!batchedNext()) {
                        return false;
                    }
                    assert current.isComplete();
                    found = true;
                    additionalFilter.reset();
                }
                // we call furtherFilter in usual way outside of this batch because it is more effective
                if (additionalFilter.next()) {
                    return true;
                }
                found = false;
            }
        }
        
        private static Cursor get(Future<Cursor> f) {
            try {
                return f.get();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
        
        private boolean batchedNext() {
            if (current == null) {
                // after last
                return false;
            }
            // go next
            current = current.next;
            if (current == null) {
                return false;
            }
            current.prev = null;
        
            final int lastJfId = filtersCount - 1;
            
            int jfId = lastJfId;
            while (current.row(jfId) == null) {
                // lookup for the first non fetched filter for the current row
                jfId--;
            }
            
            for (;;) {
                fetchCurrent(jfId);
                
                if (!current.isDropped()) {
                    // if current was not dropped then it must be fetched successfully
                    if (jfId == lastJfId) {
                        // the whole join row is ready to be returned
                        return true;
                    }
                    JoinFilter join = filters[jfId + 1];
                    if (join.isBatchFull()) {
                        // get future cursors for join and go right to fetch them
                        current = join.find(current);
                    }
                    if (current.row(join.id) != null) {
                        // either find called or outer join with null row
                        jfId = join.id;
                        continue;
                    }
                }
                // we have to go down and fetch next cursors for jfId if it is possible
                if (current.next == null) {
                    // either dropped or null-row
                    if (current.isDropped()) {
                        current = current.prev;
                        if (current == null) {
                            return false;
                        }
                    }
                    assert !current.isDropped();
                    assert jfId != lastJfId;
                    
                    jfId = 0;
                    while (current.row(jfId) != null) {
                        jfId++;
                    }
                    // force find on half filled batch (there must be either searchRows 
                    // or Cursor.EMPTY set for null-rows)
                    current = filters[jfId].find(current);
                } else {
                    // here we don't care if the current was dropped
                    current = current.next;
                    assert !current.isRow(jfId);
                    while (current.row(jfId) == null) {
                        assert jfId != top.id;
                        // need to go left and fetch more search rows
                        jfId--;
                        assert !current.isRow(jfId);
                    }
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        private void fetchCurrent(final int jfId) {
            assert current.prev == null || current.prev.isRow(jfId) : "prev must be already fetched";
            assert jfId == 0 || current.isRow(jfId - 1) : "left must be already fetched";

            assert !current.isRow(jfId) : "double fetching";

            Object x = current.row(jfId);
            assert x != null : "x null";

            final JoinFilter jf = filters[jfId];
            // in case of outer join we don't have any future around empty cursor
            boolean newCursor = x == EMPTY_CURSOR;

            if (!newCursor && current.isFuture(jfId)) {
                // get cursor from a future
                x = get((Future<Cursor>) x);
                current.updateRow(jfId, x, JoinRow.S_FUTURE, JoinRow.S_CURSOR);
                newCursor = true;
            }

            Cursor c = (Cursor) x;
            assert c != null;
            JoinFilter join = jf.join;

            for (;;) {
                if (c == null || !c.next()) {
                    if (newCursor && jf.isOuterJoin()) {
                        // replace cursor with null-row
                        current.updateRow(jfId, jf.getNullRow(), JoinRow.S_CURSOR, JoinRow.S_ROW);
                        c = null;
                        newCursor = false;
                    } else {
                        // cursor is done, drop it
                        current.drop();
                        return;
                    }
                }
                if (!jf.isOk(c == null)) {
                    // try another row from the cursor
                    continue;
                }
                boolean joinEmpty = false;
                if (join != null && !join.collectSearchRows()) {
                    if (join.isOuterJoin()) {
                        joinEmpty = true;
                    } else {
                        // join will fail, try next row in the cursor
                        continue;
                    }
                }
                if (c != null) {
                    current = current.copyBehind(jfId);
                    // get current row from cursor
                    current.updateRow(jfId, c.get(), JoinRow.S_CURSOR, JoinRow.S_ROW);
                }
                if (joinEmpty) {
                    current.updateRow(join.id, EMPTY_CURSOR, JoinRow.S_NULL, JoinRow.S_CURSOR);
                }
                return;
            }
        }

        @Override
        public String toString() {
            return "JoinBatch->\nprev->" + (current == null ? null : current.prev) +
                    "\ncurr->" + current + 
                    "\nnext->" + (current == null ? null : current.next);
        }
    }
    
    /**
     * Table filter participating in batched join.
     */
    private static final class JoinFilter {
        final TableFilter filter;
        final JoinFilter join;
        int id;
        
        IndexLookupBatch lookupBatch;
        
        private JoinFilter(IndexLookupBatch lookupBatch, TableFilter filter, JoinFilter join) {
            this.filter = filter;
            this.join = join;
            this.lookupBatch = lookupBatch != null ? lookupBatch : new FakeLookupBatch(filter);
        }
        
        public Row getNullRow() {
            return filter.table.getNullRow();
        }

        private boolean isOuterJoin() {
            return filter.joinOuter;
        }

        private boolean isBatchFull() {
            return lookupBatch.isBatchFull();
        }

        private boolean isOk(boolean ignoreJoinCondition) {
            boolean filterOk = filter.isOk(filter.filterCondition);
            boolean joinOk = filter.isOk(filter.joinCondition);

            return filterOk && (ignoreJoinCondition || joinOk);
        }
        
        private boolean collectSearchRows() {
            assert !isBatchFull();
            IndexCursor c = filter.cursor;
            c.prepare(filter.session, filter.indexConditions);
            if (c.isAlwaysFalse()) {
                return false;
            }
            lookupBatch.addSearchRows(c.getStart(), c.getEnd());
            return true;
        }
        
        private JoinRow find(JoinRow current) {
            assert current != null;

            // lookupBatch is allowed to be empty when we have some null-rows and forced find call
            List<Future<Cursor>> result = lookupBatch.find();

            // go backwards and assign futures
            for (int i = result.size(); i > 0;) {
                assert current.isRow(id - 1);
                if (current.row(id) == EMPTY_CURSOR) {
                    // outer join support - skip row with existing empty cursor
                    current = current.prev;
                    continue;
                }
                assert current.row(id) == null;
                Future<Cursor> future = result.get(--i);
                if (future == null) {
                    current.updateRow(id, EMPTY_CURSOR, JoinRow.S_NULL, JoinRow.S_CURSOR);
                } else {
                    current.updateRow(id, future, JoinRow.S_NULL, JoinRow.S_FUTURE);
                }
                if (current.prev == null || i == 0) {
                    break;
                }
                current = current.prev;
            }

            // handle empty cursors (because of outer joins) at the beginning
            while (current.prev != null && current.prev.row(id) == EMPTY_CURSOR) {
                current = current.prev;
            }
            assert current.prev == null || current.prev.isRow(id);
            assert current.row(id) != null;
            assert !current.isRow(id);

            // the last updated row
            return current;
        }
        
        @Override
        public String toString() {
            return "JoinFilter->" + filter;
        }
    }
    
    /**
     * Linked row in batched join.
     */
    private static final class JoinRow {
        private static final long S_NULL = 0;
        private static final long S_FUTURE = 1;
        private static final long S_CURSOR = 2;
        private static final long S_ROW = 3;

        private static final long S_MASK = 3;

        /**
         * May contain one of the following:
         * <br/>- {@code null}: means that we need to get future cursor for this row
         * <br/>- {@link Future}: means that we need to get a new {@link Cursor} from the {@link Future}
         * <br/>- {@link Cursor}: means that we need to fetch {@link Row}s from the {@link Cursor}
         * <br/>- {@link Row}: the {@link Row} is already fetched and is ready to be used
         */
        Object[] row;
        long state;

        JoinRow prev;
        JoinRow next;

        /**
         * @param row Row.
         */
        private JoinRow(Object[] row) {
            this.row = row;
        }

        /**
         * @param joinFilterId Join filter id.
         * @return Row state.
         */
        private long getState(int joinFilterId) {
            return (state >>> (joinFilterId << 1)) & S_MASK;
        }

        /**
         * Allows to do a state transition in the following order:
         * 0. Slot contains {@code null} ({@link #S_NULL}).
         * 1. Slot contains {@link Future} ({@link #S_FUTURE}).
         * 2. Slot contains {@link Cursor} ({@link #S_CURSOR}).
         * 3. Slot contains {@link Row} ({@link #S_ROW}).
         *
         * @param joinFilterId {@link JoinRow} filter id.
         * @param i Increment by this number of moves.
         */
        private void incrementState(int joinFilterId, long i) {
            assert i > 0 : i;
            state += i << (joinFilterId << 1);
        }

        private void updateRow(int joinFilterId, Object x, long oldState, long newState) {
            assert getState(joinFilterId) == oldState : "old state: " + getState(joinFilterId);
            row[joinFilterId] = x;
            incrementState(joinFilterId, newState - oldState);
            assert getState(joinFilterId) == newState : "new state: " + getState(joinFilterId);
        }

        private Object row(int joinFilterId) {
            return row[joinFilterId];
        }

        private boolean isRow(int joinFilterId) {
            return getState(joinFilterId) == S_ROW;
        }

        private boolean isFuture(int joinFilterId) {
            return getState(joinFilterId) == S_FUTURE;
        }

        private boolean isCursor(int joinFilterId) {
            return getState(joinFilterId) == S_CURSOR;
        }

        private boolean isComplete() {
            return isRow(row.length - 1);
        }

        private boolean isDropped() {
            return row == null;
        }

        private void drop() {
            if (prev != null) {
                prev.next = next;
            }
            if (next != null) {
                next.prev = prev;
            }
            row = null;
        }
        
        /**
         * Copy this JoinRow behind itself in linked list of all in progress rows.
         *
         * @param jfId The last fetched filter id.
         * @return The copy.
         */
        private JoinRow copyBehind(int jfId) {
            assert isCursor(jfId);
            assert jfId + 1 == row.length || row[jfId + 1] == null;
            
            Object[] r = new Object[row.length];
            if (jfId != 0) {
                System.arraycopy(row, 0, r, 0, jfId);
            }
            JoinRow copy = new JoinRow(r);
            copy.state = state;
            
            if (prev != null) {
                copy.prev = prev;
                prev.next = copy;
            }
            prev = copy;
            copy.next = this;
            
            return copy;
        }
        
        @Override
        public String toString() {
            return "JoinRow->" + Arrays.toString(row);
        }
    }

    /**
     * Fake Lookup batch for indexes which do not support batching but have to participate 
     * in batched joins.
     */
    private static class FakeLookupBatch implements IndexLookupBatch {
        final TableFilter filter;

        SearchRow first;
        SearchRow last;

        boolean full;

        final List<Future<Cursor>> result = new SingletonList<Future<Cursor>>();

        /**
         * @param index Index.
         */
        public FakeLookupBatch(TableFilter filter) {
            this.filter = filter;
        }

        @Override
        public void addSearchRows(SearchRow first, SearchRow last) {
            assert !full;
            this.first = first;
            this.last = last;
            full = true;
        }

        @Override
        public boolean isBatchFull() {
            return full;
        }

        @Override
        public List<Future<Cursor>> find() {
            if (!full) {
                return Collections.emptyList();
            }
            Cursor c = filter.getIndex().find(filter, first, last);
            result.set(0, new DoneFuture<Cursor>(c));
            full = false;
            first = last = null;
            return result;
        }
    }

    /**
     * Simple singleton list.
     */
    private static class SingletonList<E> extends AbstractList<E> {
        private E element;

        @Override
        public E get(int index) {
            assert index == 0;
            return element;
        }

        @Override
        public E set(int index, E element) {
            assert index == 0;
            this.element = element;
            return null;
        }

        @Override
        public int size() {
            return 1;
        }
    }
}
