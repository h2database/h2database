/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.ResultSetMetaData;
import java.util.Objects;

import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.command.ddl.SequenceOptions;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.SequenceValue;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.util.HasSQL;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;
import org.h2.value.ValueUuid;

/**
 * This class represents a column in a table.
 */
public class Column implements HasSQL {

    /**
     * The name of the rowid pseudo column.
     */
    public static final String ROWID = "_ROWID_";

    /**
     * This column is not nullable.
     */
    public static final int NOT_NULLABLE =
            ResultSetMetaData.columnNoNulls;

    /**
     * This column is nullable.
     */
    public static final int NULLABLE =
            ResultSetMetaData.columnNullable;

    /**
     * It is not know whether this column is nullable.
     */
    public static final int NULLABLE_UNKNOWN =
            ResultSetMetaData.columnNullableUnknown;

    private TypeInfo type;
    private Table table;
    private String name;
    private int columnId;
    private boolean nullable = true;
    private Expression defaultExpression;
    private Expression onUpdateExpression;
    private String originalSQL;
    private SequenceOptions autoIncrementOptions;
    private boolean convertNullToDefault;
    private Sequence sequence;
    private boolean isGenerated;
    private GeneratedColumnResolver generatedTableFilter;
    private int selectivity;
    private String comment;
    private boolean primaryKey;
    private boolean visible = true;
    private boolean rowId;
    private Domain domain;

    /**
     * Appends the specified columns to the specified builder.
     *
     * @param builder
     *            string builder
     * @param columns
     *            columns
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeColumns(StringBuilder builder, Column[] columns, int sqlFlags) {
        for (int i = 0, l = columns.length; i < l; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            columns[i].getSQL(builder, sqlFlags);
        }
        return builder;
    }

    /**
     * Appends the specified columns to the specified builder.
     *
     * @param builder
     *            string builder
     * @param columns
     *            columns
     * @param separator
     *            separator
     * @param suffix
     *            additional SQL to append after each column
     * @param sqlFlags
     *            formatting flags
     * @return the specified string builder
     */
    public static StringBuilder writeColumns(StringBuilder builder, Column[] columns, String separator,
            String suffix, int sqlFlags) {
        for (int i = 0, l = columns.length; i < l; i++) {
            if (i > 0) {
                builder.append(separator);
            }
            columns[i].getSQL(builder, sqlFlags).append(suffix);
        }
        return builder;
    }

    public Column(String name, int valueType) {
        this(name, TypeInfo.getTypeInfo(valueType));
    }

    public Column(String name, TypeInfo type) {
        this.name = name;
        this.type = type;
    }

    public Column(String name, TypeInfo type, String originalSQL) {
        this.name = name;
        this.type = type;
        this.originalSQL = originalSQL;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof Column)) {
            return false;
        }
        Column other = (Column) o;
        if (table == null || other.table == null ||
                name == null || other.name == null) {
            return false;
        }
        if (table != other.table) {
            return false;
        }
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        if (table == null || name == null) {
            return 0;
        }
        return table.getId() ^ name.hashCode();
    }

    public Column getClone() {
        Column newColumn = new Column(name, type);
        newColumn.copy(this);
        return newColumn;
    }

    /**
     * Convert a value to this column's type without precision and scale checks.
     *
     * @param provider the cast information provider
     * @param v the value
     * @return the value
     */
    public Value convert(CastDataProvider provider, Value v) {
        try {
            return v.convertTo(type, provider, this);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                e = getDataConversionError(v, e);
            }
            throw e;
        }
    }

    /**
     * Returns whether this column is a generated column.
     *
     * @return whether this column is a generated column
     */
    public boolean getGenerated() {
        return isGenerated;
    }

    /**
     * Set the default value in the form of a generated expression of other
     * columns.
     *
     * @param expression the computed expression
     */
    public void setGeneratedExpression(Expression expression) {
        this.isGenerated = true;
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
    public void setDefaultExpression(Session session, Expression defaultExpression) {
        // also to test that no column names are used
        if (defaultExpression != null) {
            defaultExpression = defaultExpression.optimize(session);
            if (defaultExpression.isConstant()) {
                defaultExpression = ValueExpression.get(
                        defaultExpression.getValue(session));
            }
        }
        this.defaultExpression = defaultExpression;
    }

    /**
     * Set the on update expression.
     *
     * @param session the session
     * @param onUpdateExpression the on update expression
     */
    public void setOnUpdateExpression(Session session, Expression onUpdateExpression) {
        // also to test that no column names are used
        if (onUpdateExpression != null) {
            onUpdateExpression = onUpdateExpression.optimize(session);
            if (onUpdateExpression.isConstant()) {
                onUpdateExpression = ValueExpression.get(onUpdateExpression.getValue(session));
            }
        }
        this.onUpdateExpression = onUpdateExpression;
    }

    public int getColumnId() {
        return columnId;
    }

    @Override
    public String getSQL(int sqlFlags) {
        return rowId ? name : Parser.quoteIdentifier(name, sqlFlags);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return rowId ? builder.append(name) : Parser.quoteIdentifier(builder, name, sqlFlags);
    }

    /**
     * Appends the table name and column name to the specified builder.
     *
     * @param builder the string builder
     * @param sqlFlags formatting flags
     * @return the specified string builder
     */
    public StringBuilder getSQLWithTable(StringBuilder builder, int sqlFlags) {
        return getSQL(table.getSQL(builder, sqlFlags).append('.'), sqlFlags);
    }

    public String getName() {
        return name;
    }

    public TypeInfo getType() {
        return type;
    }

    public void setNullable(boolean b) {
        nullable = b;
    }

    public boolean getVisible() {
        return visible;
    }

    public void setVisible(boolean b) {
        visible = b;
    }

    public Domain getDomain() {
        return domain;
    }

    public void setDomain(Domain domain) {
        this.domain = domain;
    }

    /**
     * Returns whether this column is a row identity column.
     *
     * @return true for _ROWID_ column, false otherwise
     */
    public boolean isRowId() {
        return rowId;
    }

    /**
     * Set row identity flag.
     *
     * @param rowId true _ROWID_ column, false otherwise
     */
    public void setRowId(boolean rowId) {
        this.rowId = rowId;
    }

    /**
     * Validate the value, convert it if required, and update the sequence value
     * if required. If the value is null, the default value (NULL if no default
     * is set) is returned. Domain constraints are validated as well.
     *
     * @param session the session
     * @param value the value or null
     * @param row the row
     * @return the new or converted value
     */
    public Value validateConvertUpdateSequence(Session session, Value value, Row row) {
        Expression localDefaultExpression = defaultExpression;
        boolean addKey = false;
        if (value == null) {
            if (localDefaultExpression == null) {
                if (!nullable) {
                    throw DbException.get(ErrorCode.NULL_NOT_ALLOWED, name);
                }
                value = ValueNull.INSTANCE;
            } else {
                if (isGenerated) {
                    synchronized (this) {
                        generatedTableFilter.set(row);
                        try {
                            value = localDefaultExpression.getValue(session);
                        } finally {
                            generatedTableFilter.set(null);
                        }
                    }
                } else {
                    value = localDefaultExpression.getValue(session);
                }
                if (value == ValueNull.INSTANCE && !nullable) {
                    throw DbException.get(ErrorCode.NULL_NOT_ALLOWED, name);
                }
                addKey = true;
            }
        } else if (value == ValueNull.INSTANCE) {
            if (convertNullToDefault) {
                value = localDefaultExpression.getValue(session);
                addKey = true;
            }
            if (value == ValueNull.INSTANCE && !nullable) {
                throw DbException.get(ErrorCode.NULL_NOT_ALLOWED, name);
            }
        }
        try {
            value = value.convertForAssignTo(type, session, name);
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.DATA_CONVERSION_ERROR_1) {
                e = getDataConversionError(value, e);
            }
            throw e;
        }
        if (domain != null) {
            domain.checkConstraints(session, value);
        }
        if (addKey && !localDefaultExpression.isConstant() && primaryKey) {
            session.setLastIdentity(value);
        }
        updateSequenceIfRequired(session, value);
        return value;
    }

    private DbException getDataConversionError(Value value, DbException cause) {
        StringBuilder builder = new StringBuilder().append(value.getTraceSQL()).append(" (");
        if (table != null) {
            builder.append(table.getName()).append(": ");
        }
        builder.append(getCreateSQL()).append(')');
        return DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, cause, builder.toString());
    }

    private void updateSequenceIfRequired(Session session, Value value) {
        if (sequence != null) {
            long current = sequence.getCurrentValue();
            long inc = sequence.getIncrement();
            long now = value.getLong();
            boolean update = false;
            if (inc > 0 && now > current) {
                update = true;
            } else if (inc < 0 && now < current) {
                update = true;
            }
            if (update) {
                sequence.modify(null, now + inc, null, null, null);
                session.setLastIdentity(ValueBigint.get(now));
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
    public void convertAutoIncrementToSequence(Session session, Schema schema,
            int id, boolean temporary) {
        if (autoIncrementOptions == null) {
            DbException.throwInternalError();
        }
        if ("IDENTITY".equals(originalSQL)) {
            originalSQL = "BIGINT";
        } else if ("SERIAL".equals(originalSQL)) {
            originalSQL = "INT";
        }
        String sequenceName;
        do {
            ValueUuid uuid = ValueUuid.getNewRandom();
            String s = uuid.getString();
            s = StringUtils.toUpperEnglish(s.replace('-', '_'));
            sequenceName = "SYSTEM_SEQUENCE_" + s;
        } while (schema.findSequence(sequenceName) != null);
        Sequence seq = new Sequence(session, schema, id, sequenceName, autoIncrementOptions, true);
        seq.setTemporary(temporary);
        session.getDatabase().addSchemaObject(session, seq);
        setAutoIncrementOptions(null);
        SequenceValue seqValue = new SequenceValue(seq, null);
        setDefaultExpression(session, seqValue);
        setSequence(seq);
    }

    /**
     * Prepare all expressions of this column.
     *
     * @param session the session
     */
    public void prepareExpression(Session session) {
        if (defaultExpression != null) {
            if (isGenerated) {
                generatedTableFilter = new GeneratedColumnResolver(table);
                defaultExpression.mapColumns(generatedTableFilter, 0, Expression.MAP_INITIAL);
            }
            defaultExpression = defaultExpression.optimize(session);
        }
        if (onUpdateExpression != null) {
            onUpdateExpression = onUpdateExpression.optimize(session);
        }
    }

    public String getCreateSQLWithoutName() {
        return getCreateSQL(false);
    }

    public String getCreateSQL() {
        return getCreateSQL(true);
    }

    private String getCreateSQL(boolean includeName) {
        StringBuilder buff = new StringBuilder();
        if (includeName && name != null) {
            Parser.quoteIdentifier(buff, name, DEFAULT_SQL_FLAGS).append(' ');
        }
        if (originalSQL != null) {
            buff.append(originalSQL);
        } else {
            type.getSQL(buff);
        }

        if (!visible) {
            buff.append(" INVISIBLE ");
        }

        if (defaultExpression != null) {
            if (isGenerated) {
                buff.append(" GENERATED ALWAYS AS ");
                defaultExpression.getEnclosedSQL(buff, DEFAULT_SQL_FLAGS);
            } else {
                buff.append(" DEFAULT ");
                defaultExpression.getSQL(buff, DEFAULT_SQL_FLAGS);
            }
        }
        if (onUpdateExpression != null) {
            buff.append(" ON UPDATE ");
            onUpdateExpression.getSQL(buff, DEFAULT_SQL_FLAGS);
        }
        if (convertNullToDefault) {
            buff.append(" NULL_TO_DEFAULT");
        }
        if (sequence != null) {
            buff.append(" SEQUENCE ");
            sequence.getSQL(buff, DEFAULT_SQL_FLAGS);
        }
        if (selectivity != 0) {
            buff.append(" SELECTIVITY ").append(selectivity);
        }
        if (comment != null) {
            buff.append(" COMMENT ");
            StringUtils.quoteStringSQL(buff, comment);
        }
        if (!nullable) {
            buff.append(" NOT NULL");
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

    public Expression getOnUpdateExpression() {
        return onUpdateExpression;
    }

    public boolean isAutoIncrement() {
        return autoIncrementOptions != null;
    }

    /**
     * Set the autoincrement flag and related options of this column.
     *
     * @param sequenceOptions
     *            sequence options, or {@code null} to reset the flag
     */
    public void setAutoIncrementOptions(SequenceOptions sequenceOptions) {
        this.autoIncrementOptions = sequenceOptions;
        this.nullable = false;
        if (sequenceOptions != null) {
            convertNullToDefault = true;
        }
    }

    /**
     * Returns autoincrement options, or {@code null}.
     *
     * @return autoincrement options, or {@code null}
     */
    public SequenceOptions getAutoIncrementOptions() {
        return autoIncrementOptions;
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

    String getDefaultSQL() {
        return defaultExpression == null ? null : defaultExpression.getSQL(DEFAULT_SQL_FLAGS);
    }

    String getOnUpdateSQL() {
        return onUpdateExpression == null ? null : onUpdateExpression.getSQL(DEFAULT_SQL_FLAGS);
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
        if (onUpdateExpression != null && !onUpdateExpression.isEverything(visitor)) {
            return false;
        }
        return true;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Check whether the new column is of the same type and not more restricted
     * than this column.
     *
     * @param newColumn the new (target) column
     * @return true if the new column is compatible
     */
    public boolean isWideningConversion(Column newColumn) {
        TypeInfo newType = newColumn.type;
        int valueType = type.getValueType();
        if (valueType != newType.getValueType()) {
            return false;
        }
        long precision = type.getPrecision();
        long newPrecision = newType.getPrecision();
        if (precision > newPrecision
                || precision < newPrecision && (valueType == Value.CHAR || valueType == Value.BINARY)) {
            return false;
        }
        if (type.getScale() != newType.getScale()) {
            return false;
        }
        if (!Objects.equals(type.getExtTypeInfo(), newType.getExtTypeInfo())) {
            return false;
        }
        if (nullable && !newColumn.nullable) {
            return false;
        }
        if (convertNullToDefault != newColumn.convertNullToDefault) {
            return false;
        }
        if (primaryKey != newColumn.primaryKey) {
            return false;
        }
        if (autoIncrementOptions != null || newColumn.autoIncrementOptions != null) {
            return false;
        }
        if (domain != newColumn.domain) {
            return false;
        }
        if (convertNullToDefault || newColumn.convertNullToDefault) {
            return false;
        }
        if (defaultExpression != null || newColumn.defaultExpression != null) {
            return false;
        }
        if (isGenerated || newColumn.isGenerated) {
            return false;
        }
        if (onUpdateExpression != null || newColumn.onUpdateExpression != null) {
            return false;
        }
        return true;
    }

    /**
     * Copy the data of the source column into the current column.
     *
     * @param source the source column
     */
    public void copy(Column source) {
        name = source.name;
        type = source.type;
        // table is not set
        // columnId is not set
        nullable = source.nullable;
        defaultExpression = source.defaultExpression;
        onUpdateExpression = source.onUpdateExpression;
        originalSQL = source.originalSQL;
        // autoIncrement, start, increment is not set
        convertNullToDefault = source.convertNullToDefault;
        sequence = source.sequence;
        comment = source.comment;
        generatedTableFilter = source.generatedTableFilter;
        isGenerated = source.isGenerated;
        selectivity = source.selectivity;
        primaryKey = source.primaryKey;
        visible = source.visible;
    }

}
