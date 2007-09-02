/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import org.h2.command.dml.Select;
import org.h2.constant.SysProperties;
import org.h2.engine.Mode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexCondition;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * @author Thomas
 */
public class TableFilter implements ColumnResolver {
    private static final int BEFORE_FIRST = 0, FOUND = 1, AFTER_LAST = 2, NULL_ROW = 3;
    private final Table table;
    private final String alias;
    private final Select select;
    private Session session;
    private Index index;
    private Cursor cursor;
    private int scanCount;
    private boolean used; // used in the plan

    // conditions that can be used for direct index lookup (start or end)
    private final ObjectArray indexConditions = new ObjectArray();

    // conditions that can't be used for index lookup, but for row filter for this table (ID=ID, NAME LIKE '%X%')
    private Expression filterCondition;

    // the complete join condition
    private Expression joinCondition;
    private SearchRow currentSearchRow;
    private Row current;
    private int state;
    
    private ObjectArray joins;
    // private TableFilter join;
    
    private boolean outerJoin;
    private boolean foundOne;
    private Expression fullCondition;

    public TableFilter(Session session, Table table, String alias, boolean rightsChecked, Select select)
            throws SQLException {
        this.session = session;
        this.table = table;
        this.alias = alias;
        this.select = select;
        if (!rightsChecked) {
            session.getUser().checkRight(table, Right.SELECT);
        }
    }

    public Select getSelect() {
        return select;
    }

    public Table getTable() {
        return table;
    }

    public void lock(Session session, boolean exclusive, boolean force) throws SQLException {
        table.lock(session, exclusive, force);
        for (int i = 0; joins != null && i < joins.size(); i++) {
            getTableFilter(i).lock(session, exclusive, force);
        }
    }

    private TableFilter getTableFilter(int i) {
        return (TableFilter) joins.get(i);
    }

    public PlanItem getBestPlanItem(Session session) throws SQLException {
        PlanItem item;
        if (indexConditions.size() == 0) {
            item = new PlanItem();
            item.setIndex(table.getScanIndex(session));
            item.cost = item.getIndex().getCost(session, null);
        } else {
            int len = table.getColumns().length;
            int[] masks = new int[len];
            for (int i = 0; i < indexConditions.size(); i++) {
                IndexCondition condition = (IndexCondition) indexConditions.get(i);
                if (condition.isEvaluatable()) {
                    if (condition.isAlwaysFalse()) {
                        masks = null;
                        break;
                    } else {
                        int id = condition.getColumn().getColumnId();
                        masks[id] |= condition.getMask();
                    }
                }
            }
            item = table.getBestPlanItem(session, masks);
        }
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            setEvaluatable(join);
            item.setJoinPlan(join.getBestPlanItem(session));
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
            join = join.getJoin();
        } while (join != null);
    }

    public void setPlanItem(PlanItem item) {
        this.index = item.getIndex();
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            if (item.getJoinPlan() != null) {
                join.setPlanItem(item.getJoinPlan());
            }
        }
    }

    public void prepare() throws SQLException {
        // forget all unused index conditions
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition condition = (IndexCondition) indexConditions.get(i);
            if (!condition.isAlwaysFalse()) {
                Column col = condition.getColumn();
                if (index.getColumnIndex(col) < 0) {
                    indexConditions.remove(i);
                    i--;
                }
            }
        }
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            if (SysProperties.CHECK && join == this) {
                throw Message.getInternalError("self join");
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

    public void startQuery(Session session) throws SQLException {
        this.session = session;
        scanCount = 0;
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            join.startQuery(session);
        }
    }

    public void reset() {
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            join.reset();
        }
        state = BEFORE_FIRST;
        foundOne = false;
    }

    public boolean next() throws SQLException {
        boolean alwaysFalse = false;
        if (state == AFTER_LAST) {
            return false;
        } else if (state == BEFORE_FIRST) {
            SearchRow start = null, end = null;
            for (int i = 0; i < indexConditions.size(); i++) {
                IndexCondition condition = (IndexCondition) indexConditions.get(i);
                if (condition.isAlwaysFalse()) {
                    alwaysFalse = true;
                    break;
                }
                Column column = condition.getColumn();
                int type = column.getType();
                int id = column.getColumnId();
                Value v = condition.getCurrentValue(session).convertTo(type);
                if (condition.isStart()) {
                    // TODO index: start.setExpression(id,
                    // bigger(start.getValue(id), e));
                    if (start == null) {
                        start = table.getTemplateRow();
                    }
                    start.setValue(id, v);
                }
                if (condition.isEnd()) {
                    // TODO index: end.setExpression(id,
                    // smaller(end.getExpression(id), e));
                    if (end == null) {
                        end = table.getTemplateRow();
                    }
                    end.setValue(id, v);
                }
            }
            if (!alwaysFalse) {
                cursor = index.find(session, start, end);
                for (int i = 0; joins != null && i < joins.size(); i++) {
                    TableFilter join = getTableFilter(i);
                    join.reset();
                }
            }
        } else {
            // state == FOUND || LAST_ROW
            // the last row was ok - try next row of the join
            boolean found = joins != null;
            for (int i = 0; joins != null && i < joins.size(); i++) {
                TableFilter join = getTableFilter(i);
                if (!join.next()) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return true;
            }
        }
        while (true) {
            // go to the next row
            if (state == NULL_ROW) {
                break;
            }
            if (alwaysFalse) {
                state = AFTER_LAST;
            } else {
                scanCount++;
                if (cursor.next()) {
                    currentSearchRow = cursor.getSearchRow();
                    current = null;
                    // cursor.get();
                    state = FOUND;
                } else {
                    state = AFTER_LAST;
                }
            }
            // if no more rows found, try the null row (for outer joins only)
            if (state == AFTER_LAST) {
                if (outerJoin && !foundOne) {
                    state = NULL_ROW;
                    current = table.getNullRow();
                    currentSearchRow = current;
                } else {
                    break;
                }
            }
            if (!isOk(filterCondition)) {
                continue;
            }
            boolean joinConditionOk = isOk(joinCondition);
            if (state == FOUND && joinConditionOk) {
                foundOne = true;
            }
            for (int i = 0; joins != null && i < joins.size(); i++) {
                TableFilter join = getTableFilter(i);
                join.reset();
            }
            boolean doContinue = false;
            for (int i = 0; joins != null && i < joins.size(); i++) {
                TableFilter join = getTableFilter(i);
                if (!join.next()) {
                    doContinue = true;
                }
            }
            if (doContinue) {
                continue;
            }
            // check if it's ok
            if (state == NULL_ROW || joinConditionOk) {
                return true;
            }
        }
        state = AFTER_LAST;
        return false;
    }

    private boolean isOk(Expression condition) throws SQLException {
        if (condition == null) {
            return true;
        }
        return Boolean.TRUE.equals(condition.getBooleanValue(session));
    }

    public Row get() throws SQLException {
        if (current == null && currentSearchRow != null) {
            if (table.isClustered()) {
                current = table.getTemplateRow();
                for (int i = 0; i < currentSearchRow.getColumnCount(); i++) {
                    current.setValue(i, currentSearchRow.getValue(i));
                }
            } else {
                current = cursor.get();
            }
        }
        return current;
    }

    public void set(Row current) {
        // this is currently only used so that check constraints work - to set
        // the current (new) row
        this.current = current;
        this.currentSearchRow = current;
    }

    public String getTableAlias() {
        if (alias != null) {
            return alias;
        }
        return table.getName();
    }

    public void addIndexCondition(IndexCondition condition) {
        indexConditions.add(condition);
    }

    public void addFilterCondition(Expression condition, boolean join) {
        if (join) {
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

    public void addJoin(TableFilter filter, boolean outer, Expression on) throws SQLException {
        if (on != null) {
            on.mapColumns(this, 0);
        }
        if (joins == null) {
            this.joins = new ObjectArray();
            joins.add(filter);
            filter.outerJoin = outer;
            if (on != null) {
                filter.mapAndAddFilter(on);
            }
        } else {
            int todoAddJoinNestedOrSameLevel;
            TableFilter join = getTableFilter(0);
            join.addJoin(filter, outer, on);
        }
    }

    private void mapAndAddFilter(Expression on) throws SQLException {
        on.mapColumns(this, 0);
        addFilterCondition(on, true);
        on.createIndexConditions(session, this);
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            join.mapAndAddFilter(on);
        }
    }

    public TableFilter getJoin() {
        int todoGetJoin;
        return joins == null ? null : getTableFilter(0);
    }

    public boolean isJoinOuter() {
        return outerJoin;
    }

    public String getPlanSQL(boolean join) {
        StringBuffer buff = new StringBuffer();
        if (join) {
            if (outerJoin) {
                buff.append("LEFT OUTER JOIN ");
            } else {
                buff.append("INNER JOIN ");
            }
        }
        buff.append(table.getSQL());
        if (alias != null && !table.getName().equals(alias)) {
            buff.append(' ');
            buff.append(alias);
        }
        buff.append(" /* ");
        StringBuffer planBuff = new StringBuffer();
        planBuff.append(index.getPlanSQL());
        if (indexConditions.size() > 0) {
            planBuff.append(": ");
            for (int i = 0; i < indexConditions.size(); i++) {
                IndexCondition condition = (IndexCondition) indexConditions.get(i);
                if (i > 0) {
                    planBuff.append(" AND ");
                }
                planBuff.append(condition.getSQL());
            }
        }
        String plan = planBuff.toString();
        plan = StringUtils.quoteRemarkSQL(plan);
        buff.append(plan);
        buff.append(" */");
        if (joinCondition != null) {
            buff.append(" ON ");
            buff.append(StringUtils.unEnclose(joinCondition.getSQL()));
        }
        if (filterCondition != null) {
            buff.append(" /* WHERE ");
            String condition = StringUtils.unEnclose(filterCondition.getSQL());
            condition = StringUtils.quoteRemarkSQL(condition);
            buff.append(condition);
            buff.append("*/");
        }
        return buff.toString();
    }

    public void removeUnusableIndexConditions() {
        for (int i = 0; i < indexConditions.size(); i++) {
            IndexCondition cond = (IndexCondition) indexConditions.get(i);
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
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public boolean getUsed() {
        return used;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public void removeJoin() {
        int todoRemoveJoin;
        this.joins = null;
    }

    public Expression getJoinCondition() {
        return joinCondition;
    }

    public void removeJoinCondition() {
        this.joinCondition = null;
    }

    public Expression getFilterCondition() {
        return filterCondition;
    }

    public void removeFilterCondition() {
        this.filterCondition = null;
    }

    public void setFullCondition(Expression condition) {
        this.fullCondition = condition;
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            join.setFullCondition(condition);
        }
    }

    public void optimizeFullCondition(boolean fromOuterJoin) {
        if (fullCondition != null) {
            fullCondition.addFilterConditions(this, fromOuterJoin || outerJoin);
            for (int i = 0; joins != null && i < joins.size(); i++) {
                TableFilter join = getTableFilter(i);
                join.optimizeFullCondition(fromOuterJoin || outerJoin);
            }
        }
    }

    public void setEvaluatable(TableFilter filter, boolean b) {
        if (filterCondition != null) {
            filterCondition.setEvaluatable(filter, b);
        }
        if (joinCondition != null) {
            joinCondition.setEvaluatable(filter, b);
        }
        for (int i = 0; joins != null && i < joins.size(); i++) {
            TableFilter join = getTableFilter(i);
            join.setEvaluatable(filter, b);
        }
    }

    public String getSchemaName() {
        return table.getSchema().getName();
    }

    public Column[] getColumns() {
        return table.getColumns();
    }

    public Column[] getSystemColumns() {
        if (!Mode.getCurrentMode().systemColumns) {
            return null;
        }
        Column[] sys = new Column[3];
        sys[0] = new Column("oid", Value.INT, ValueInt.PRECISION, 0);
        sys[0].setTable(table, 0);
        sys[1] = new Column("ctid", Value.STRING, ValueInt.PRECISION, 0);
        sys[1].setTable(table, 0);
        sys[2] = new Column("CTID", Value.STRING, ValueInt.PRECISION, 0);
        sys[2].setTable(table, 0);
        return sys;
    }

    public Value getValue(Column column) throws SQLException {
        if (SysProperties.INDEX_LOOKUP_NEW) {
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
        } else {
            if (currentSearchRow == null) {
                return null;
            }
            if (current == null) {
                current = cursor.get();
            }
            int columnId = column.getColumnId();
            return current.getValue(columnId);
        }
    }

    public TableFilter getTableFilter() {
        return this;
    }

}
