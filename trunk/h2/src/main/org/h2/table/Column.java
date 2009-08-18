/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.SequenceValue;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

/**
 * This class represents a column in a table.
 */
public class Column {

    /**
     * This column is not nullable.
     */
    public static final int NOT_NULLABLE = ResultSetMetaData.columnNoNulls;

    /**
     * This column is nullable.
     */
    public static final int NULLABLE = ResultSetMetaData.columnNullable;

    /**
     * It is not know whether this column is nullable.
     */
    public static final int NULLABLE_UNKNOWN = ResultSetMetaData.columnNullableUnknown;

    private final int type;
    private final long precision;
    private final int scale;
    private final int displaySize;
    private Table table;
    private String name;
    private int columnId;
    private boolean nullable = true;
    private Expression defaultExpression;
    private Expression checkConstraint;
    private String checkConstraintSQL;
    private String originalSQL;
    private boolean autoIncrement;
    private long start;
    private long increment;
    private boolean convertNullToDefault;
    private Sequence sequence;
    private boolean isComputed;
    private TableFilter computeTableFilter;
    private int selectivity;
    private SingleColumnResolver resolver;
    private String comment;
    private boolean primaryKey;

    public Column(String name, int type) {
        this(name, type, -1, -1, -1);
    }

    public Column(String name, int type, long precision, int scale, int displaySize) {
        this.name = name;
        this.type = type;
        if (precision == -1 && scale == -1 && displaySize == -1) {
            DataType dt = DataType.getDataType(type);
            precision = dt.defaultPrecision;
            scale = dt.defaultScale;
            displaySize = dt.defaultDisplaySize;
        }
        this.precision = precision;
        this.scale = scale;
        this.displaySize = displaySize;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Column)) {
            return false;
        }
        Column other = (Column) o;
        if (table == null || other.table == null || name == null || other.name == null) {
            return false;
        }
        return table == other.table && name.equals(other.name);
    }

    public int hashCode() {
        if (table == null || name == null) {
            return 0;
        }
        return table.getId() ^ name.hashCode();
    }

    public Column getClone() {
        Column newColumn = new Column(name, type, precision, scale, displaySize);
        // table is not set
        // columnId is not set
        newColumn.nullable = nullable;
        newColumn.defaultExpression = defaultExpression;
        newColumn.originalSQL = originalSQL;
        // autoIncrement, start, increment is not set
        newColumn.convertNullToDefault = convertNullToDefault;
        newColumn.sequence = sequence;
        newColumn.comment = comment;
        newColumn.computeTableFilter = computeTableFilter;
        newColumn.isComputed = isComputed;
        newColumn.selectivity = selectivity;
        newColumn.primaryKey = primaryKey;
        return newColumn;
    }

    boolean getComputed() {
        return isComputed;
    }

    /**
     * Compute the value of this computed column.
     *
     * @param session the session
     * @param row the row
     * @return the value
     */
    Value computeValue(Session session, Row row) throws SQLException {
        synchronized (this) {
            computeTableFilter.setSession(session);
            computeTableFilter.set(row);
            return defaultExpression.getValue(session);
        }
    }

    /**
     * Set the default value in the form of a computed expression of other
     * columns.
     *
     * @param expression the computed expression
     */
    public void setComputedExpression(Expression expression) {
        this.isComputed = true;
        this.defaultExpression = expression;
    }

    /**
     * Set the table and column id.
     *
     * @param table the table
     * @param columnId the column index
     */
    public void setTable(Table table, int columnId) {
        this.table = table;
        this.columnId = columnId;
    }

    public Table getTable() {
        return table;
    }

    /**
     * Set the default expression.
     *
     * @param session the session
     * @param defaultExpression the default expression
     */
    public void setDefaultExpression(Session session, Expression defaultExpression) throws SQLException {
        // also to test that no column names are used
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
            if (defaultExpression.isConstant()) {
                defaultExpression = ValueExpression.get(defaultExpression.getValue(session));
            }
        }
        this.defaultExpression = defaultExpression;
    }

    public int getColumnId() {
        return columnId;
    }

    public String getSQL() {
        return Parser.quoteIdentifier(name);
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public long getPrecision() {
        return precision;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    public int getScale() {
        return scale;
    }

    public void setNullable(boolean b) {
        nullable = b;
    }

    /**
     * Validate the value, convert it if required, and update the sequence value
     * if required. If the value is null, the default value (NULL if no default
     * is set) is returned. Check constraints are validated as well.
     *
     * @param session the session
     * @param value the value or null
     * @return the new or converted value
     */
    public Value validateConvertUpdateSequence(Session session, Value value) throws SQLException {
        if (value == null) {
            if (defaultExpression == null) {
                value = ValueNull.INSTANCE;
            } else {
                synchronized (this) {
                    value = defaultExpression.getValue(session).convertTo(type);
                }
                if (primaryKey) {
                    session.setLastIdentity(value);
                }
            }
        }
        Mode mode = session.getDatabase().getMode();
        if (value == ValueNull.INSTANCE) {
            if (convertNullToDefault) {
                synchronized (this) {
                    value = defaultExpression.getValue(session).convertTo(type);
                }
            }
            if (value == ValueNull.INSTANCE && !nullable) {
                if (mode.convertInsertNullToZero) {
                    DataType dt = DataType.getDataType(type);
                    if (dt.decimal) {
                        value = ValueInt.get(0).convertTo(type);
                    } else if (dt.type == Value.TIMESTAMP) {
                        value = ValueTimestamp.getNoCopy(new Timestamp(System.currentTimeMillis()));
                    } else if (dt.type == Value.TIME) {
                        // need to normalize
                        value = ValueTime.get(Time.valueOf("0:0:0"));
                    } else if (dt.type == Value.DATE) {
                        value = ValueTimestamp.getNoCopy(new Timestamp(System.currentTimeMillis())).convertTo(dt.type);
                    } else {
                        value = ValueString.get("").convertTo(type);
                    }
                } else {
                    throw Message.getSQLException(ErrorCode.NULL_NOT_ALLOWED, name);
                }
            }
        }
        if (checkConstraint != null) {
            resolver.setValue(value);
            Value v;
            synchronized (this) {
                v = checkConstraint.getValue(session);
            }
            // Both TRUE and NULL are ok
            if (Boolean.FALSE.equals(v.getBoolean())) {
                throw Message.getSQLException(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1, checkConstraint.getSQL());
            }
        }
        value = value.convertScale(mode.convertOnlyToSmallerScale, scale);
        if (precision > 0) {
            if (!value.checkPrecision(precision)) {
                String s = value.getTraceSQL();
                if (s.length() > 127) {
                    s = s.substring(0, 128) + "...";
                }
                throw Message.getSQLException(ErrorCode.VALUE_TOO_LONG_2,
                        getCreateSQL(), s + " (" + value.getPrecision() + ")");
            }
        }
        updateSequenceIfRequired(session, value);
        return value;
    }

    private void updateSequenceIfRequired(Session session, Value value) throws SQLException {
        if (sequence != null) {
            long current = sequence.getCurrentValue();
            long increment = sequence.getIncrement();
            long now = value.getLong();
            boolean update = false;
            if (increment > 0 && now > current) {
                update = true;
            } else if (increment < 0 && now < current) {
                update = true;
            }
            if (update) {
                sequence.setStartValue(now + increment);
                session.setLastIdentity(ValueLong.get(now));
                sequence.flush(session);
            }
        }
    }

    /**
     * Convert the auto-increment flag to a sequence that is linked with this
     * table.
     *
     * @param session the session
     * @param schema the schema where the sequence should be generated
     * @param id the object id
     * @param temporary true if the sequence is temporary and does not need to
     *            be stored
     */
    public void convertAutoIncrementToSequence(Session session, Schema schema, int id, boolean temporary)
            throws SQLException {
        if (!autoIncrement) {
            Message.throwInternalError();
        }
        if ("IDENTITY".equals(originalSQL)) {
            originalSQL = "BIGINT";
        }
        String sequenceName;
        for (int i = 0;; i++) {
            ValueUuid uuid = ValueUuid.getNewRandom();
            String s = uuid.getString();
            s = s.replace('-', '_').toUpperCase();
            sequenceName = "SYSTEM_SEQUENCE_" + s;
            if (schema.findSequence(sequenceName) == null) {
                break;
            }
        }
        Sequence sequence = new Sequence(schema, id, sequenceName, true);
        sequence.setStartValue(start);
        sequence.setIncrement(increment);
        if (!temporary) {
            session.getDatabase().addSchemaObject(session, sequence);
        }
        setAutoIncrement(false, 0, 0);
        SequenceValue seq = new SequenceValue(sequence);
        setDefaultExpression(session, seq);
        setSequence(sequence);
    }

    /**
     * Prepare all expressions of this column.
     *
     * @param session the session
     */
    public void prepareExpression(Session session) throws SQLException {
        if (defaultExpression != null) {
            computeTableFilter = new TableFilter(session, table, null, false, null);
            defaultExpression.mapColumns(computeTableFilter, 0);
            defaultExpression = defaultExpression.optimize(session);
        }
    }

    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder();
        if (name != null) {
            buff.append(Parser.quoteIdentifier(name)).append(' ');
        }
        if (originalSQL != null) {
            buff.append(originalSQL);
        } else {
            buff.append(DataType.getDataType(type).name);
            switch (type) {
            case Value.DECIMAL:
                buff.append('(').append(precision).append(", ").append(scale).append(')');
                break;
            case Value.BYTES:
            case Value.STRING:
            case Value.STRING_IGNORECASE:
            case Value.STRING_FIXED:
                if (precision < Integer.MAX_VALUE) {
                    buff.append('(').append(precision).append(')');
                }
                break;
            default:
            }
        }
        if (defaultExpression != null) {
            String sql = defaultExpression.getSQL();
            if (sql != null) {
                if (isComputed) {
                    buff.append(" AS ").append(sql);
                } else if (defaultExpression != null) {
                    buff.append(" DEFAULT ").append(sql);
                }
            }
        }
        if (!nullable) {
            buff.append(" NOT NULL");
        }
        if (convertNullToDefault) {
            buff.append(" NULL_TO_DEFAULT");
        }
        if (sequence != null) {
            buff.append(" SEQUENCE ").append(sequence.getSQL());
        }
        if (selectivity != 0) {
            buff.append(" SELECTIVITY ").append(selectivity);
        }
        if (checkConstraint != null) {
            buff.append(" CHECK ").append(checkConstraintSQL);
        }
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        return buff.toString();
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setOriginalSQL(String original) {
        originalSQL = original;
    }

    public String getOriginalSQL() {
        return originalSQL;
    }

    public Expression getDefaultExpression() {
        return defaultExpression;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    /**
     * Set the autoincrement flag and related properties of this column.
     *
     * @param autoInc the new autoincrement flag
     * @param start the sequence start value
     * @param increment the sequence increment
     */
    public void setAutoIncrement(boolean autoInc, long start, long increment) {
        this.autoIncrement = autoInc;
        this.start = start;
        this.increment = increment;
        this.nullable = false;
        if (autoInc) {
            convertNullToDefault = true;
        }
    }

    public void setConvertNullToDefault(boolean convert) {
        this.convertNullToDefault = convert;
    }

    /**
     * Rename the column. This method will only set the column name to the new
     * value.
     *
     * @param newName the new column name
     */
    public void rename(String newName) {
        this.name = newName;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public Sequence getSequence() {
        return sequence;
    }

    /**
     * Get the selectivity of the column. Selectivity 100 means values are
     * unique, 10 means every distinct value appears 10 times on average.
     *
     * @return the selectivity
     */
    public int getSelectivity() {
        return selectivity == 0 ? Constants.SELECTIVITY_DEFAULT : selectivity;
    }

    /**
     * Set the new selectivity of a column.
     *
     * @param selectivity the new value
     */
    public void setSelectivity(int selectivity) {
        selectivity = selectivity < 0 ? 0 : (selectivity > 100 ? 100 : selectivity);
        this.selectivity = selectivity;
    }

    /**
     * Add a check constraint expression to this column. An existing check
     * constraint constraint is added using AND.
     *
     * @param session the session
     * @param expr the (additional) constraint
     */
    public void addCheckConstraint(Session session, Expression expr) throws SQLException {
        resolver = new SingleColumnResolver(this);
        synchronized (this) {
            String oldName = name;
            if (name == null) {
                name = "VALUE";
            }
            expr.mapColumns(resolver, 0);
            name = oldName;
        }
        expr = expr.optimize(session);
        resolver.setValue(ValueNull.INSTANCE);
        // check if the column is mapped
        synchronized (this) {
            expr.getValue(session);
        }
        if (checkConstraint == null) {
            checkConstraint = expr;
        } else {
            checkConstraint = new ConditionAndOr(ConditionAndOr.AND, checkConstraint, expr);
        }
        checkConstraintSQL = getCheckConstraintSQL(session, name);
    }

    /**
     * Get the check constraint expression for this column if set.
     *
     * @param session the session
     * @param asColumnName the column name to use
     * @return the constraint expression
     */
    public Expression getCheckConstraint(Session session, String asColumnName) throws SQLException {
        if (checkConstraint == null) {
            return null;
        }
        Parser parser = new Parser(session);
        String sql;
        synchronized (this) {
            String oldName = name;
            name = asColumnName;
            sql = checkConstraint.getSQL();
            name = oldName;
        }
        Expression expr = parser.parseExpression(sql);
        return expr;
    }

    String getDefaultSQL() {
        return defaultExpression == null ? null : defaultExpression.getSQL();
    }

    int getPrecisionAsInt() {
        return MathUtils.convertLongToInt(precision);
    }

    DataType getDataType() {
        return DataType.getDataType(type);
    }

    /**
     * Get the check constraint SQL snippet.
     *
     * @param session the session
     * @param asColumnName the column name to use
     * @return the SQL snippet
     */
    String getCheckConstraintSQL(Session session, String asColumnName) throws SQLException {
        Expression constraint = getCheckConstraint(session, asColumnName);
        return constraint == null ? "" : constraint.getSQL();
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * Visit the default expression, the check constraint, and the sequence (if
     * any).
     *
     * @param visitor the visitor
     * @return true if every visited expression returned true, or if there are
     *         no expressions
     */
    boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.GET_DEPENDENCIES) {
            if (sequence != null) {
                visitor.getDependencies().add(sequence);
            }
        }
        if (defaultExpression != null && !defaultExpression.isEverything(visitor)) {
            return false;
        }
        if (checkConstraint != null && !checkConstraint.isEverything(visitor)) {
            return false;
        }
        return true;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public String toString() {
        return name;
    }

}
